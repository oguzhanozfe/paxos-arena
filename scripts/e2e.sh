#!/usr/bin/env bash
# End-to-end test of the play API: three arena processes on 127.0.0.1 and the
# client SDK's harness (docs/UNITY-INTEGRATION.md section 11.6).
#
#   run 1  the whole flow on a new tournament: sessions, join, deals, moves
#          with one illegal move rejected, finishes, a completed intent resent
#          with its key (the recorded response comes back unchanged), a stale
#          and a skipped sequence number, a client restarted with an intent in
#          flight, leaderboard, close, settle, events, claims, and the
#          tournament's ledger read back through the operator API.
#   run 2  the same flow on another tournament, with the leader process killed
#          (SIGKILL) in the middle of a round while a move's answer has not
#          reached the client. The client is rebuilt from its store, resends
#          the move with the same key, learns the new leader from a
#          follower's 307 (on the move itself, or on a session refresh that
#          reached the follower first), receives the recorded result from the
#          new leader, and the flow finishes with exactly one claim per paid
#          player in the ledger.
#
# After each run every replica must report the same applied slot and state
# hash; after run 2 the killed replica is restarted from its wal file first.
#
# Exits 0 with a SKIP line when no .NET SDK is installed. Environment:
#   E2E_PORT_BASE    operator ports are base+1..3, play ports base+11..13 (38080)
#   E2E_SESSION_TTL  -session-ttl of the replicas (30s: the client refreshes
#                    its session every 15 s, so each run crosses a refresh)
#   E2E_KEEP=1       keep the work directory (logs, wal files, client stores);
#                    it is also kept when a step fails
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HARNESS_PROJECT="$ROOT/unity-client/Tests~/Harness/Harness.csproj"

say() { printf 'e2e: %s\n' "$*"; }
fail() {
	say "FAIL: $*"
	exit 1
}

if ! command -v dotnet >/dev/null 2>&1 || [ -z "$(dotnet --list-sdks 2>/dev/null)" ]; then
	say "SKIP: no .NET SDK found; install one (8.0 or later) to run the client harness"
	exit 0
fi
for tool in go curl; do
	command -v "$tool" >/dev/null 2>&1 || fail "$tool is required"
done

BASE=${E2E_PORT_BASE:-38080}
case "$BASE" in
'' | *[!0-9]*) fail "E2E_PORT_BASE must be a number" ;;
esac
[ "$BASE" -ge 1024 ] && [ "$BASE" -le 65500 ] || fail "E2E_PORT_BASE must be between 1024 and 65500"

SESSION_TTL=${E2E_SESSION_TTL:-30s}
TMP=${TMPDIR:-/tmp}
WORK=$(mktemp -d "${TMP%/}/paxos-arena-e2e.XXXXXX")
NODES="1 2 3"

op_url() { printf 'http://127.0.0.1:%d' $((BASE + $1)); }
play_url() { printf 'http://127.0.0.1:%d' $((BASE + 10 + $1)); }
node_pid() { cat "$WORK/node$1.pid" 2>/dev/null || true; }

# stop_node sends SIGTERM, waits up to 10 s for a graceful stop, then SIGKILL.
stop_node() {
	local pid
	pid=$(node_pid "$1")
	[ -n "$pid" ] || return 0
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		local n=0
		while kill -0 "$pid" 2>/dev/null && [ $n -lt 100 ]; do
			sleep 0.1
			n=$((n + 1))
		done
		kill -9 "$pid" 2>/dev/null || true
	fi
	rm -f "$WORK/node$1.pid"
}

cleanup() {
	local status=$?
	trap - EXIT INT TERM
	for i in $NODES; do
		stop_node "$i"
	done
	if [ "$status" -ne 0 ] || [ "${E2E_KEEP:-0}" = 1 ]; then
		say "work directory kept: $WORK"
	else
		rm -rf "$WORK"
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for i in $NODES; do
	for port in $((BASE + i)) $((BASE + 10 + i)); do
		if port_in_use "$port"; then
			fail "port $port is in use; set E2E_PORT_BASE to use other ports"
		fi
	done
done

random_hex32() {
	if command -v openssl >/dev/null 2>&1; then
		openssl rand -hex 32
	else
		od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
	fi
}

say "work directory $WORK"
say "building arena"
(cd "$ROOT" && go build -o "$WORK/arena" ./cmd/arena)

say "building the client harness"
SDK_MAJOR=$(dotnet --list-sdks | sed -n 's/^\([0-9][0-9]*\)\..*/\1/p' | sort -n | tail -1)
TFM_ARG=()
if [ -n "$SDK_MAJOR" ] && [ "$SDK_MAJOR" -lt 10 ]; then
	TFM_ARG=("-p:HarnessTargetFramework=net$SDK_MAJOR.0")
fi
dotnet build "$HARNESS_PROJECT" --nologo -v quiet -o "$WORK/harness" ${TFM_ARG[@]+"${TFM_ARG[@]}"} >"$WORK/harness-build.log" 2>&1 ||
	{
		cat "$WORK/harness-build.log"
		fail "the harness did not build"
	}

(
	umask 077
	printf 'k1=%s\n' "$(random_hex32)" >"$WORK/session-keys"
	random_hex32 >"$WORK/deal-secret"
)

PEERS="1=$(op_url 1),2=$(op_url 2),3=$(op_url 3)"
PLAY_URLS="1=$(play_url 1),2=$(play_url 2),3=$(play_url 3)"
PLAY_LIST="$(play_url 1),$(play_url 2),$(play_url 3)"
OP_LIST="$(op_url 1),$(op_url 2),$(op_url 3)"

start_node() {
	"$WORK/arena" -id "$1" -listen "127.0.0.1:$((BASE + $1))" -wal "$WORK/node$1.wal" -peers "$PEERS" \
		-play-listen "127.0.0.1:$((BASE + 10 + $1))" -play-urls "$PLAY_URLS" \
		-session-keys-file "$WORK/session-keys" -deal-secret-file "$WORK/deal-secret" -session-ttl "$SESSION_TTL" \
		-log-level warn >>"$WORK/node$1.log" 2>&1 &
	echo $! >"$WORK/node$1.pid"
	# Not a job of this shell: a replica killed on purpose is not reported.
	disown $!
}

# node_status prints GET /v1/node of node $1 without whitespace, or nothing.
node_status() {
	curl -fsS --max-time 2 "$(op_url "$1")/v1/node" 2>/dev/null | tr -d ' \n\t' || true
}
json_field() { sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p"; }

# wait_leader prints the id of a ready leader, waiting up to 30 s.
wait_leader() {
	local n=0 s
	while [ $n -lt 300 ]; do
		for i in $NODES; do
			s=$(node_status "$i")
			case "$s" in
			*'"role":"leader"'*'"ready":true'*)
				echo "$i"
				return 0
				;;
			esac
		done
		sleep 0.1
		n=$((n + 1))
	done
	return 1
}

# wait_converged waits up to 30 s until every replica reports the same applied
# slot and state hash, and prints them.
wait_converged() {
	local n=0 s applied hash first line
	while [ $n -lt 300 ]; do
		first="" line="" same=1
		for i in $NODES; do
			s=$(node_status "$i")
			applied=$(printf '%s' "$s" | json_field applied)
			hash=$(printf '%s' "$s" | json_field state_hash)
			if [ -z "$applied" ] || [ -z "$hash" ]; then
				same=0
				break
			fi
			line="$line node $i applied $applied hash ${hash:0:16}..."
			if [ -z "$first" ]; then
				first="$applied/$hash"
			elif [ "$first" != "$applied/$hash" ]; then
				same=0
			fi
		done
		if [ "$same" = 1 ]; then
			say "identical state on every replica:$line"
			return 0
		fi
		sleep 0.1
		n=$((n + 1))
	done
	for i in $NODES; do
		say "node $i: $(node_status "$i")"
	done
	return 1
}

cat >"$WORK/kill-leader.sh" <<EOF
#!/bin/sh
# Kills the process of the ready leader with SIGKILL (written by scripts/e2e.sh).
n=0
while [ \$n -lt 100 ]; do
	for i in $NODES; do
		s=\$(curl -fsS --max-time 2 "http://127.0.0.1:\$(($BASE + i))/v1/node" 2>/dev/null | tr -d ' \n\t')
		case "\$s" in
		*'"role":"leader"'*'"ready":true'*)
			pid=\$(cat "$WORK/node\$i.pid")
			kill -9 "\$pid" || exit 1
			echo "\$i" >"$WORK/killed"
			echo "killed node \$i (pid \$pid) with SIGKILL"
			exit 0
			;;
		esac
	done
	sleep 0.1
	n=\$((n + 1))
done
echo "no ready leader to kill" >&2
exit 1
EOF
chmod +x "$WORK/kill-leader.sh"

harness() {
	local label=$1
	shift
	say "$label: dotnet PaxosArena.Client.Harness.dll --play $PLAY_LIST --operator $OP_LIST --players 3 $*"
	dotnet "$WORK/harness/PaxosArena.Client.Harness.dll" --play "$PLAY_LIST" --operator "$OP_LIST" --players 3 \
		--store "$WORK/stores-$label" "$@" 2>&1 | tee "$WORK/harness-$label.log"
}

say "starting 3 replicas: operator $OP_LIST, play $PLAY_LIST, session tokens for $SESSION_TTL"
for i in $NODES; do
	start_node "$i"
done
leader=$(wait_leader) || fail "no leader was elected within 30 s"
say "node $leader leads"

harness run1 || fail "run 1: the harness failed"
wait_converged || fail "run 1: the replicas did not converge"

harness run2 --kill-leader-command "$WORK/kill-leader.sh" || fail "run 2: the harness failed"
[ -f "$WORK/killed" ] || fail "run 2: the leader was never killed"
killed=$(cat "$WORK/killed")
say "restarting node $killed from $WORK/node$killed.wal"
start_node "$killed"
wait_converged || fail "run 2: the restarted replica did not converge"

say "PASS"
