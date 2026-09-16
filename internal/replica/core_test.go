package replica

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

func rules() tournament.Rules {
	return tournament.Rules{
		EntryFee: 100, RakeBps: 500, PrizeBps: []uint32{10000}, MinEntrants: 1, MaxEntrants: 10,
		MaxScore: 1000, MinAge: 18, TieBreak: tournament.EarliestSubmission,
	}
}

func createCmd(key, id string) tournament.Command {
	return tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: 1,
		Op: tournament.CreateTournament{ID: tournament.TournamentID(id), Seed: 9, Rules: rules()}}
}

// singleNode builds a one-node core on store and drives it to leadership.
func singleNode(t *testing.T, store replog.Store) *Core {
	t.Helper()
	c, err := NewCore(replog.DefaultConfig(1, []paxos.NodeID{1}), store, rand.New(rand.NewPCG(1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	c.Tick(0)
	for now := time.Duration(0); now < 2*time.Second && c.Log().Role() != replog.Leader; now += 10 * time.Millisecond {
		c.Tick(now)
	}
	if !c.Log().Ready() {
		t.Fatalf("single node did not become a ready leader: role %s", c.Log().Role())
	}
	return c
}

func TestSingleNodeCoreAppliesInOrder(t *testing.T) {
	c := singleNode(t, replog.NewMemStore())
	if _, err := c.Submit(time.Second, createCmd("k1", "t1")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(time.Second, createCmd("k2", "t1")); err != nil {
		t.Fatal(err)
	}
	applied := c.ApplyCommitted()
	if len(applied) != 3 {
		t.Fatalf("applied %d slots, want the leadership no-op and two commands: %+v", len(applied), applied)
	}
	if !applied[0].NoOp || applied[0].Slot != 1 || applied[0].Key != "" {
		t.Errorf("slot 1 should be the leadership no-op: %+v", applied[0])
	}
	if applied[1].Key != "k1" || applied[1].Result.Code != tournament.OK || applied[1].Result.Slot != 2 {
		t.Errorf("slot 2: %+v", applied[1])
	}
	if applied[2].Key != "k2" || applied[2].Result.Code != tournament.TournamentExists {
		t.Errorf("slot 3: %+v", applied[2])
	}
	if got := c.ApplyCommitted(); len(got) != 0 {
		t.Errorf("second ApplyCommitted applied %d more", len(got))
	}
	if _, ok := c.ApplyNext(); ok {
		t.Error("ApplyNext reported a slot after catching up")
	}
	st := c.Status()
	if st.Self != 1 || st.Leader != 1 || st.Role != replog.Leader || !st.Ready || st.CommitIndex != 3 || st.Applied != 3 || st.Tournaments != 1 {
		t.Errorf("Status = %+v", st)
	}
	if st.StateHash != tournament.Digest(c.State().Hash()) {
		t.Error("Status hash differs from the state hash")
	}
}

// TestReplayFromStore is the S2 layer test: a core rebuilt from the same
// store applies the same prefix and reaches the same hash.
func TestReplayFromStore(t *testing.T) {
	store := replog.NewMemStore()
	c := singleNode(t, store)
	for i, key := range []string{"a", "b", "c"} {
		cmd := createCmd(key, "t"+key)
		if i == 1 {
			cmd.Op = tournament.Join{Tournament: "ta", Player: tournament.Player{ID: "p", Jurisdiction: "TR", Age: 30}}
		}
		if _, err := c.Submit(time.Second, cmd); err != nil {
			t.Fatal(err)
		}
	}
	c.ApplyCommitted()
	want := c.State().Hash()
	again, err := NewCore(replog.DefaultConfig(1, []paxos.NodeID{1}), store, rand.New(rand.NewPCG(2, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if again.State().Applied() != 0 {
		t.Fatal("a fresh core should start with an empty state machine")
	}
	applied := again.ApplyCommitted()
	if len(applied) != 4 {
		t.Fatalf("replayed %d slots, want 4", len(applied))
	}
	if again.State().Hash() != want {
		t.Error("replay from the store gave a different hash")
	}
	if again.Log().Role() != replog.Follower {
		t.Error("a rebuilt core should start as a follower")
	}
	ta, ok := again.State().Tournament("ta")
	if !ok || len(ta.Entries) != 1 {
		t.Errorf("replayed tournament: %+v, %t", ta, ok)
	}
}

func TestApplyNextSkipsUndecodableEntries(t *testing.T) {
	store := replog.NewMemStore()
	b := paxos.Ballot{Round: 1, Node: 1}
	store.SaveChosen(replog.Entry{Slot: 1, Ballot: b, Value: paxos.Value("not json")})
	v, _ := tournament.Encode(createCmd("k", "t"))
	store.SaveChosen(replog.Entry{Slot: 2, Ballot: b, Value: paxos.Value(v)})
	store.SaveChosen(replog.Entry{Slot: 3, Ballot: b})
	c, err := NewCore(replog.DefaultConfig(1, []paxos.NodeID{1}), store, rand.New(rand.NewPCG(1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	a, ok := c.ApplyNext()
	if !ok || !a.NoOp || a.Err == nil || a.Slot != 1 {
		t.Errorf("undecodable slot: %+v, %t", a, ok)
	}
	a, ok = c.ApplyNext()
	if !ok || a.NoOp || a.Key != "k" || a.Result.Code != tournament.OK {
		t.Errorf("valid slot: %+v, %t", a, ok)
	}
	a, ok = c.ApplyNext()
	if !ok || !a.NoOp || a.Err != nil || a.Slot != 3 {
		t.Errorf("no-op slot: %+v, %t", a, ok)
	}
	if _, ok := c.ApplyNext(); ok {
		t.Error("ApplyNext past the commit index")
	}
	if c.State().Applied() != 3 {
		t.Errorf("Applied = %d", c.State().Applied())
	}
}

func TestSubmitErrors(t *testing.T) {
	c, err := NewCore(replog.DefaultConfig(1, []paxos.NodeID{1, 2, 3}), replog.NewMemStore(), rand.New(rand.NewPCG(1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Submit(0, createCmd("k", "t"))
	var nl replog.NotLeaderError
	if !errors.As(err, &nl) {
		t.Errorf("Submit on a follower = %v, want NotLeaderError", err)
	}
	if _, err := c.Submit(0, tournament.Command{Key: "", Op: tournament.Close{Tournament: "t"}}); err == nil {
		t.Error("Submit accepted an empty key")
	}
	if _, err := c.Submit(0, tournament.Command{Key: "k"}); err == nil {
		t.Error("Submit accepted a nil op")
	}
	if _, _, err := c.ReadIndex(0); !errors.As(err, &nl) {
		t.Errorf("ReadIndex on a follower = %v", err)
	}
	if c.Failed() != nil {
		t.Error("Failed on a healthy core")
	}
}

func TestNewCoreRejectsBadConfig(t *testing.T) {
	if _, err := NewCore(replog.Config{}, replog.NewMemStore(), rand.New(rand.NewPCG(1, 1))); err == nil {
		t.Error("NewCore accepted an empty config")
	}
}
