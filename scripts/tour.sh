#!/usr/bin/env bash
# A guided ten-minute tour of paxos-arena: three replicas as separate processes, one paid
# tournament from creation to payout, the leader killed before settlement, the dead replica
# restarted from its log file, then one seeded run of the fault-injecting simulator.
# Every step prints what it demonstrates. Needs Go, curl and python3.
#
#   scripts/tour.sh                 run everything
#   TOUR_PORT_BASE=47080 scripts/tour.sh
#   TOUR_PAUSE=1 scripts/tour.sh    wait for Enter between steps
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_PORT=${TOUR_PORT_BASE:-47080}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/paxos-arena-tour.XXXXXX")
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT INT TERM

url() { echo "http://127.0.0.1:$((BASE_PORT + $1))"; }
PEERS="1=$(url 1),2=$(url 2),3=$(url 3)"
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1], {}, {'d': d}))" "$1"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; [ "${TOUR_PAUSE:-0}" = 1 ] && read -r -p "   (Enter to run) " _ || true; }
shows() { printf '   \033[2m%s\033[0m\n' "$@"; }
post() { # post <node> <path> <idempotency key> <json body>  -> prints "STATUS BODY"
  curl -s -o "$WORK/body" -w '%{http_code}' -X POST "$(url "$1")$2" -H "Idempotency-Key: $3" \
    -H 'Content-Type: application/json' -d "$4"; printf ' '; cat "$WORK/body"
}
start_node() {
  "$WORK/arena" -id "$1" -listen "127.0.0.1:$((BASE_PORT + $1))" -wal "$WORK/node$1.wal" -peers "$PEERS" \
    -log-level error > "$WORK/node$1.log" 2>&1 &
  PIDS[$1]=$!
}
leader() { curl -s -m 2 "$(url "$1")/v1/node" | json 'd["leader"]'; }

step "1. Build the service"
go build -o "$WORK/arena" ./cmd/arena
shows "One static binary, Go standard library only."

step "2. Start three replicas as separate processes, each with its own write-ahead log file"
for n in 1 2 3; do start_node "$n"; done
for _ in $(seq 1 50); do L=$(leader 1 2>/dev/null || true); [ -n "${L:-}" ] && [ "$L" != 0 ] && break; sleep 0.2; done
echo "   leader elected: node $L"
shows "Each replica is a Multi-Paxos acceptor, learner and possible leader. A follower that hears no" \
      "heartbeat for 150-300 ms runs Phase 1 (Prepare/Promise) and becomes leader with a majority."

step "3. Create a paid tournament through a follower"
F=$(( L % 3 + 1 ))
RULES='{"id":"t1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,"max_score":100000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":7,"jurisdictions":["XX"]}}}'
R=$(post "$F" /v1/tournaments create-t1 "$RULES"); echo "   node $F answered ${R%% *}; chosen in log slot $(echo "${R#* }" | json 'd["slot"]')"
shows "Any node accepts commands: node $F forwarded to leader $L, which proposed the command; it was" \
      "chosen when a majority accepted it, and every replica applies it in slot order."

step "4. Three players join (each pays a 500 entry fee into the pool)"
for p in p1 p2 p3; do R=$(post "$F" /v1/tournaments/t1/entries "join-$p" "{\"player\":{\"id\":\"$p\",\"jurisdiction\":\"TR\",\"age\":31}}"); echo "   $p: ${R%% *} slot $(echo "${R#* }" | json 'd["slot"]')"; done
SEED=$(echo "${R#* }" | json 'd["seed"]')

step "5. The phone retries a join whose answer it never received"
R=$(post "$F" /v1/tournaments/t1/entries join-p1 '{"player":{"id":"p1","jurisdiction":"TR","age":31}}')
echo "   same Idempotency-Key, same body -> ${R%% *} replayed=$(echo "${R#* }" | json 'd["replayed"]')"
R=$(post "$F" /v1/tournaments/t1/entries join-p1 '{"player":{"id":"p1","jurisdiction":"TR","age":99}}')
echo "   same key, different body       -> ${R%% *} $(echo "${R#* }" | json 'd["code"]')"
shows "The replicated results table remembers every key's result, so a retry after any crash replays" \
      "the recorded answer and charges nothing twice. Reusing a key for another command is refused."

step "6. Scores arrive, the tournament closes (standings and pool computed once, inside the log)"
i=0; for p in p1 p2 p3; do i=$((i+1)); post "$F" /v1/tournaments/t1/scores "score-$p" "{\"player\":\"$p\",\"score\":$((i*1000)),\"deal_seed\":$SEED}" >/dev/null; done
R=$(post "$F" /v1/tournaments/t1/close close-t1 '{}')
echo "   ${R%% *}: fees $(echo "${R#* }" | json 'd["tournament"]["fees"]'), rake $(echo "${R#* }" | json 'd["tournament"]["rake"]'), pool $(echo "${R#* }" | json 'd["tournament"]["pool"]')"

step "7. Kill the leader (SIGKILL) right before settlement"
L=$(leader "$F"); kill -9 "${PIDS[$L]}"; wait "${PIDS[$L]}" 2>/dev/null || true; PIDS[$L]=""
echo "   node $L killed"
S=$(( L % 3 + 1 ))
shows "This is the double-payout moment of the single-database design: a worker dies mid-payout."

step "8. Settle through a survivor, retrying with the SAME key until a new leader answers"
for attempt in $(seq 1 40); do
  R=$(post "$S" /v1/tournaments/t1/settle settle-t1 '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}')
  code=${R%% *}; [ "$code" = 200 ] && break
  [ "$attempt" = 1 ] && echo "   attempt 1: $code ($(echo "${R#* }" | json 'd.get("code")')) - no leader yet, retrying"
  sleep 0.25
done
echo "   attempt $attempt: $code, new leader node $(leader "$S")"
echo "$R" | cut -d' ' -f2- | json '"\n".join("   paid %s place %s: %s" % (p["player"], p["place"], p["amount"]) for p in d["tournament"]["payouts"])'
R=$(post "$S" /v1/tournaments/t1/settle settle-t1 '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}')
echo "   the phone resends settle with its key -> ${R%% *} replayed=$(echo "${R#* }" | json 'd["replayed"]')"
R=$(post "$S" /v1/tournaments/t1/settle settle-t1-again '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}')
echo "   a second settle with a new key      -> ${R%% *} $(echo "${R#* }" | json 'd["code"]')"
shows "The old leader cannot get anything chosen any more (acceptors promised a higher ballot)," \
      "so there is exactly one settlement: 675 + 405 + 270 = pool 1350."

step "9. Restart the killed replica from its log file; it catches up and matches byte for byte"
start_node "$L"
for _ in $(seq 1 50); do A=$(curl -s -m 1 "$(url "$L")/v1/node" 2>/dev/null | json 'd["applied"]' 2>/dev/null || echo 0); B=$(curl -s -m 1 "$(url "$S")/v1/node" | json 'd["applied"]'); [ "$A" = "$B" ] && break; sleep 0.2; done
for n in 1 2 3; do echo "   node $n: $(curl -s "$(url "$n")/v1/node" | json '"applied slot %s, role %s, state hash %s" % (d["applied"], d["role"], d["state_hash"][:16])')"; done
shows "Same applied slot and same state hash on every replica: the deterministic state machine plus" \
      "one agreed log gives identical tournaments and ledgers everywhere."

step "10. The ledger: double-entry postings, each tagged with the log slot it came from"
curl -s "$(url "$S")/v1/tournaments/t1/ledger" | json '"\n".join("   %-9s %10s -> %-10s %4s  slot %s" % (p["kind"], p["debit"], p["credit"], p["amount"], p["slot"]) for p in d["postings"])'

step "11. The simulator: a whole 5-node cluster under crashes, partitions, loss and duplication"
go run ./cmd/chaos -seed 7 -nodes 5 -steps 40000 -log-level warn | tail -n 16
shows "Deterministic: the same seed replays the same run byte for byte. Every invariant above is" \
      "checked after every simulated event; a violation prints the seed and the replay command."

printf '\n\033[1mTour done.\033[0m Next: docs/ONBOARDING.md (code reading order), README.md (full walkthrough),\n'
printf 'scripts/e2e.sh (the Unity C# SDK against three replicas, needs .NET 8+).\n'
