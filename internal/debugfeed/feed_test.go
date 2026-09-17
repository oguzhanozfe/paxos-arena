package debugfeed

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// fixedFeed returns a feed for node 2 whose clock always reads at.
func fixedFeed(at time.Time) *Feed {
	f := New(2)
	f.now = func() time.Time { return at }
	return f
}

func encode(t *testing.T, key string, op tournament.Op) paxos.Value {
	t.Helper()
	b, err := tournament.Encode(tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: 1, Op: op})
	if err != nil {
		t.Fatal(err)
	}
	return paxos.Value(b)
}

func TestFeedRecordsMessages(t *testing.T) {
	b := paxos.Ballot{Round: 3, Node: 1}
	promised := paxos.Ballot{Round: 4, Node: 3}
	join := encode(t, "join-secret-key", tournament.Join{Tournament: "t1", Player: tournament.Player{ID: "p2", Jurisdiction: "TR", Age: 31}})
	cases := []struct {
		name string
		msg  replog.Message
		want Event // Kind, From, To, Seq, AtMs and Node are checked separately
	}{
		{"prepare", replog.Prepare{Ballot: b, FromSlot: 5},
			Event{Type: "prepare", BallotRound: 3, BallotNode: 1, Slot: 5}},
		{"promise", replog.Promise{Ballot: b, CommitIndex: 4, Accepted: []paxos.PValue{{Ballot: b, Slot: 5, Value: join}, {Ballot: b, Slot: 7}}},
			Event{Type: "promise", BallotRound: 3, BallotNode: 1, CommitIndex: 4, Detail: "accepted 2, slots 5-7"}},
		{"empty promise", replog.Promise{Ballot: b},
			Event{Type: "promise", BallotRound: 3, BallotNode: 1, Detail: "accepted 0"}},
		{"accept", replog.Accept{Ballot: b, Slot: 6, Value: join},
			Event{Type: "accept", BallotRound: 3, BallotNode: 1, Slot: 6, Value: "join t1/p2"}},
		{"accept noop", replog.Accept{Ballot: b, Slot: 6},
			Event{Type: "accept", BallotRound: 3, BallotNode: 1, Slot: 6, Value: "noop"}},
		{"accepted", replog.Accepted{Ballot: b, Slot: 6},
			Event{Type: "accepted", BallotRound: 3, BallotNode: 1, Slot: 6}},
		{"nack of an accept", replog.Nack{Ballot: b, Promised: promised, Slot: 6},
			Event{Type: "nack", BallotRound: 3, BallotNode: 1, Slot: 6, Detail: "promised r4.n3"}},
		{"nack from the lease", replog.Nack{Ballot: b, Promised: promised, LeaseRemaining: 120 * time.Millisecond},
			Event{Type: "nack", BallotRound: 3, BallotNode: 1, Detail: "promised r4.n3, lease 120ms"}},
		{"learn", replog.Learn{Ballot: b, Slot: 6, Value: join},
			Event{Type: "learn", BallotRound: 3, BallotNode: 1, Slot: 6, Value: "join t1/p2"}},
		{"learn_request", replog.LearnRequest{FromSlot: 9, MaxCount: 64},
			Event{Type: "learn_request", Slot: 9, Detail: "max 64"}},
		{"heartbeat", replog.Heartbeat{Ballot: b, CommitIndex: 8},
			Event{Type: "heartbeat", BallotRound: 3, BallotNode: 1, CommitIndex: 8}},
		{"heartbeat for a read", replog.Heartbeat{Ballot: b, CommitIndex: 8, ReadSeq: 12},
			Event{Type: "heartbeat", BallotRound: 3, BallotNode: 1, CommitIndex: 8, Detail: "read_seq 12"}},
		{"heartbeat_ack", replog.HeartbeatAck{Ballot: b, Promised: b, CommitIndex: 7, ReadSeq: 12},
			Event{Type: "heartbeat_ack", BallotRound: 3, BallotNode: 1, CommitIndex: 7, Detail: "promised r3.n1, read_seq 12"}},
	}
	at := time.UnixMilli(1_789_000_000_123)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fixedFeed(at)
			f.Sent(replog.Envelope{From: 2, To: 3, Msg: tc.msg})
			f.Received(replog.Envelope{From: 1, To: 2, Msg: tc.msg})
			p := f.Ring().Read(0, 10, true)
			if len(p.Events) != 2 {
				t.Fatalf("recorded %d events, want 2", len(p.Events))
			}
			for i, frame := range []struct {
				kind     string
				from, to paxos.NodeID
			}{{KindSend, 2, 3}, {KindRecv, 1, 2}} {
				want := tc.want
				want.Seq, want.AtMs, want.Node = uint64(i+1), at.UnixMilli(), 2
				want.Kind, want.From, want.To = frame.kind, frame.from, frame.to
				if got := p.Events[i]; got != want {
					t.Errorf("%s event:\n got %+v\nwant %+v", frame.kind, got, want)
				}
			}
		})
	}
}

func TestFeedRecordsChanges(t *testing.T) {
	at := time.UnixMilli(1_789_000_000_000)
	f := fixedFeed(at)
	b := paxos.Ballot{Round: 2, Node: 1}
	follower := replica.Status{Self: 2, Role: replog.Follower, CommitIndex: 3, Applied: 3}
	learned := follower
	learned.Leader, learned.Ballot = 1, b
	committed := learned
	committed.CommitIndex, committed.Applied = 6, 6
	create := tournament.Command{Key: "create-t1", Op: tournament.CreateTournament{ID: "t1", Seed: 99}}
	join := tournament.Command{Key: "join-p1", Op: tournament.Join{Tournament: "t1", Player: tournament.Player{ID: "p1"}}}

	f.Changed(follower, learned, nil)
	f.Changed(learned, committed, []replica.Applied{
		{Slot: 4, NoOp: true},
		{Slot: 5, Key: create.Key, Command: create, Result: tournament.Result{Code: tournament.OK, Slot: 5}},
		{Slot: 6, Key: join.Key, Command: join, Result: tournament.Result{Code: tournament.UnknownTournament, Replayed: true, Detail: "tournament \"t1\" does not exist"}},
	})
	f.Changed(committed, follower, nil) // leader lost, unknown
	lead := committed
	lead.Role, lead.Leader, lead.Ballot = replog.Leader, 2, paxos.Ballot{Round: 3, Node: 2}
	f.Changed(committed, lead, nil)
	// A change that is neither role, commit nor apply records nothing.
	hashed := lead
	hashed.Tournaments++
	f.Changed(lead, hashed, nil)

	ms := at.UnixMilli()
	want := []Event{
		{Seq: 1, AtMs: ms, Node: 2, Kind: KindRole, Type: "follower", BallotRound: 2, BallotNode: 1, CommitIndex: 3, Detail: "leader 1"},
		{Seq: 2, AtMs: ms, Node: 2, Kind: KindCommit, BallotRound: 2, BallotNode: 1, CommitIndex: 6, Detail: "from 3"},
		{Seq: 3, AtMs: ms, Node: 2, Kind: KindApplied, BallotRound: 2, BallotNode: 1, Slot: 4, CommitIndex: 6, Value: "noop"},
		{Seq: 4, AtMs: ms, Node: 2, Kind: KindApplied, BallotRound: 2, BallotNode: 1, Slot: 5, CommitIndex: 6, Value: "create_tournament t1", Detail: "ok"},
		{Seq: 5, AtMs: ms, Node: 2, Kind: KindApplied, BallotRound: 2, BallotNode: 1, Slot: 6, CommitIndex: 6, Value: "join t1/p1", Detail: "unknown_tournament, replayed"},
		{Seq: 6, AtMs: ms, Node: 2, Kind: KindRole, Type: "follower", CommitIndex: 3, Detail: "leader unknown"},
		{Seq: 7, AtMs: ms, Node: 2, Kind: KindRole, Type: "leader", BallotRound: 3, BallotNode: 2, CommitIndex: 6, Detail: "leader 2"},
	}
	got := f.Ring().Read(0, 100, true).Events
	if len(got) != len(want) {
		t.Fatalf("recorded %d events, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d:\n got %+v\nwant %+v", i+1, got[i], want[i])
		}
	}

	f = fixedFeed(at)
	f.Changed(follower, learned, []replica.Applied{{Slot: 4, NoOp: true, Err: errors.New("bad \"payload\"")}})
	if evs := f.Ring().Read(1, 10, true).Events; len(evs) != 1 || evs[0].Value != "undecodable" || evs[0].Detail != "skipped" {
		t.Errorf("undecodable slot recorded as %+v", evs)
	}
}

// TestSummaryNamesOperationAndIDsOnly: a summary carries the operation name
// and the identifiers, never the key, a seed, a verifier, a score or a move.
func TestSummaryNamesOperationAndIDsOnly(t *testing.T) {
	var verifier tournament.Digest
	for i := range verifier {
		verifier[i] = 0xab
	}
	var seed game.Seed
	for i := range seed {
		seed[i] = 0xcd
	}
	long := strings.Repeat("x", 100)
	cases := []struct {
		op   tournament.Op
		want string
	}{
		{tournament.CreateTournament{ID: "t1", Seed: 424242, Rules: tournament.Rules{EntryFee: 777777}}, "create_tournament t1"},
		{tournament.Join{Tournament: "t1", Player: tournament.Player{ID: "p2", Jurisdiction: "QQ", Age: 55}}, "join t1/p2"},
		{tournament.SubmitScore{Tournament: "t1", Player: "p2", Score: 31337, DealSeed: 424242, InputDigest: verifier}, "submit_score t1/p2"},
		{tournament.Close{Tournament: "t1"}, "close t1"},
		{tournament.Settle{Tournament: "t1", Exclusions: tournament.Exclusions{Version: 8, Jurisdictions: []string{"QQ"}}}, "settle t1"},
		{tournament.OpenSession{Device: "0123456789abcdef0123456789abcdef", Verifier: verifier, Player: "p-9", Jurisdiction: "QQ", Age: 55}, "open_session p-9"},
		{tournament.Enter{Tournament: "t1", Player: "p2", Seq: 31337}, "enter t1/p2"},
		{tournament.StartRound{Tournament: "t1", Player: "p2", Seq: 31337, Round: 2, Seed: seed}, "start_round t1/p2 r2"},
		{tournament.PlayMove{Tournament: "t1", Player: "p2", Seq: 31337, Round: 2, MoveIndex: 5}, "play_move t1/p2 r2 m5"},
		{tournament.FinishRound{Tournament: "t1", Player: "p2", Seq: 31337, Round: 3}, "finish_round t1/p2 r3"},
		{tournament.ClaimPayout{Tournament: "t1", Player: "p2", Seq: 31337}, "claim_payout t1/p2"},
		{tournament.Close{Tournament: tournament.TournamentID(long)}, "close " + long[:tournament.MaxIDLen] + "..."},
		{nil, "unknown"},
	}
	for _, tc := range cases {
		if got := SummariseOp(tc.op); got != tc.want {
			t.Errorf("SummariseOp(%T) = %q, want %q", tc.op, got, tc.want)
		}
		if tc.op == nil {
			continue
		}
		v := encode(t, "key-that-must-not-show", tc.op)
		got := SummariseValue(v)
		if got != tc.want {
			t.Errorf("SummariseValue(%T) = %q, want %q", tc.op, got, tc.want)
		}
		for _, secret := range []string{"key-that-must-not-show", "424242", "777777", "31337", "abab", "cdcd", "QQ", "55", "0123456789abcdef"} {
			if strings.Contains(got, secret) {
				t.Errorf("summary %q of %T reveals %q", got, tc.op, secret)
			}
		}
	}
	for _, tc := range []struct {
		v    paxos.Value
		want string
	}{
		{nil, "noop"},
		{paxos.Value(`{"key":"k","op":"join","body":{"tournament":"t1","player":{"id":"p1"},"secret":1}}`), "undecodable"},
		{paxos.Value("not json"), "undecodable"},
		{paxos.Value(strings.Repeat(" ", MaxSummarisedValue+1)), "value 16385 bytes"},
	} {
		if got := SummariseValue(tc.v); got != tc.want {
			t.Errorf("SummariseValue(%.20q...) = %q, want %q", tc.v, got, tc.want)
		}
	}
}
