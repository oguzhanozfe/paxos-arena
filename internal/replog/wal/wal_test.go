package wal

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

func openT(t *testing.T, path string) *File {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func mustLoad(t *testing.T, w *File) replog.Durable {
	t.Helper()
	d, err := w.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return d
}

// saveSample writes the same sequence the MemStore test uses and returns
// the Durable it must load as.
func saveSample(t *testing.T, w *File) replog.Durable {
	t.Helper()
	b1 := paxos.Ballot{Round: 1, Node: 1}
	b2 := paxos.Ballot{Round: 2, Node: 2}
	steps := []func() error{
		func() error { return w.SavePromised(b1) },
		func() error { return w.SaveMaxRound(5) },
		func() error { return w.SaveAccepted(paxos.PValue{Ballot: b1, Slot: 9, Value: paxos.Value("nine")}) },
		func() error { return w.SaveAccepted(paxos.PValue{Ballot: b1, Slot: 3, Value: paxos.Value("three")}) },
		func() error {
			return w.SaveAccepted(paxos.PValue{Ballot: b2, Slot: 9, Value: paxos.Value("nine again")})
		},
		func() error { return w.SaveChosen(replog.Entry{Slot: 4, Ballot: b1, Value: paxos.Value("four")}) },
		func() error { return w.SaveChosen(replog.Entry{Slot: 2, Ballot: b1}) },
		func() error { return w.SavePromised(b2) },
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	return replog.Durable{
		Promised: b2,
		MaxRound: 5,
		Accepted: []paxos.PValue{
			{Ballot: b1, Slot: 3, Value: paxos.Value("three")},
			{Ballot: b2, Slot: 9, Value: paxos.Value("nine again")},
		},
		Chosen: []replog.Entry{
			{Slot: 2, Ballot: b1},
			{Slot: 4, Ballot: b1, Value: paxos.Value("four")},
		},
	}
}

func TestFreshFileLoadsZero(t *testing.T) {
	w := openT(t, filepath.Join(t.TempDir(), "node.wal"))
	d := mustLoad(t, w)
	if !d.Promised.IsZero() || d.MaxRound != 0 || len(d.Accepted) != 0 || len(d.Chosen) != 0 {
		t.Fatalf("fresh file Load = %+v, want the zero Durable", d)
	}
}

func TestSavesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w := openT(t, path)
	want := saveSample(t, w)
	if got := mustLoad(t, w); !reflect.DeepEqual(got, want) {
		t.Fatalf("Load before reopen = %+v, want %+v", got, want)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.SavePromised(paxos.Ballot{Round: 9, Node: 1}); err == nil {
		t.Fatal("SavePromised after Close succeeded")
	}
	w2 := openT(t, path)
	if got := mustLoad(t, w2); !reflect.DeepEqual(got, want) {
		t.Fatalf("Load after reopen = %+v, want %+v", got, want)
	}
}

// TestTornTail is S5 for the file store: a crash in the middle of the last
// write leaves the previous durable records, never a partial one, and the
// file accepts new records after the torn tail is cut off.
func TestTornTail(t *testing.T) {
	dir := t.TempDir()
	ref := filepath.Join(dir, "ref.wal")
	w := openT(t, ref)
	want := saveSample(t, w)
	good, err := os.ReadFile(ref)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	// One more complete record, to cut in different places.
	extra := filepath.Join(dir, "extra.wal")
	if err := os.WriteFile(extra, good, 0o600); err != nil {
		t.Fatal(err)
	}
	we := openT(t, extra)
	if err := we.SaveAccepted(paxos.PValue{Ballot: paxos.Ballot{Round: 3, Node: 3}, Slot: 12, Value: paxos.Value("twelve")}); err != nil {
		t.Fatal(err)
	}
	we.Close()
	full, err := os.ReadFile(extra)
	if err != nil {
		t.Fatal(err)
	}
	last := full[len(good):]

	flipped := append([]byte(nil), last...)
	flipped[len(flipped)-2] ^= 0x40
	cases := map[string][]byte{
		"half a header":        last[:3],
		"header only":          last[:headerBytes],
		"half the payload":     last[:headerBytes+(len(last)-headerBytes)/2],
		"all but one byte":     last[:len(last)-1],
		"bad checksum":         flipped,
		"zero-filled tail":     make([]byte, 64),
		"absurd length prefix": {0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1, 2, 3},
	}
	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "torn.wal")
			if err := os.WriteFile(path, append(append([]byte(nil), good...), tail...), 0o600); err != nil {
				t.Fatal(err)
			}
			w := openT(t, path)
			if got := mustLoad(t, w); !reflect.DeepEqual(got, want) {
				t.Fatalf("Load = %+v, want the records before the torn write %+v", got, want)
			}
			if info, err := os.Stat(path); err != nil || info.Size() != int64(len(good)) {
				t.Fatalf("file size after Open = %v (%v), want the valid prefix %d", info.Size(), err, len(good))
			}
			next := paxos.Ballot{Round: 7, Node: 1}
			if err := w.SavePromised(next); err != nil {
				t.Fatalf("SavePromised after truncation: %v", err)
			}
			w.Close()
			w2 := openT(t, path)
			got := mustLoad(t, w2)
			if got.Promised != next || !reflect.DeepEqual(got.Accepted, want.Accepted) || !reflect.DeepEqual(got.Chosen, want.Chosen) {
				t.Fatalf("Load after a save on the truncated file = %+v", got)
			}
		})
	}
}

// TestCorruptionBeforeTheTailIsRefused: a damaged record followed by valid
// records is not a torn write, and truncating there would silently drop
// durable state, so Open refuses the file.
func TestCorruptionBeforeTheTailIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w := openT(t, path)
	saveSample(t, w)
	w.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[headerBytes+2] ^= 0x01 // inside the first payload
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open of a file damaged in its first record = %v, want ErrCorrupt", err)
	}
}

func TestBind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	w := openT(t, path)
	if err := w.Bind(2, []paxos.NodeID{3, 1, 2}); err != nil {
		t.Fatalf("Bind on a fresh file: %v", err)
	}
	saveSample(t, w)
	w.Close()

	w = openT(t, path)
	if m, ok := w.Membership(); !ok || m.Self != 2 || !reflect.DeepEqual(m.Peers, []paxos.NodeID{1, 2, 3}) {
		t.Fatalf("Membership = %+v, %t", m, ok)
	}
	if err := w.Bind(2, []paxos.NodeID{1, 2, 3}); err != nil {
		t.Errorf("Bind with the same membership: %v", err)
	}
	if err := w.Bind(1, []paxos.NodeID{1, 2, 3}); err == nil {
		t.Error("Bind as another node succeeded")
	}
	if err := w.Bind(2, []paxos.NodeID{1, 2, 3, 4}); err == nil {
		t.Error("Bind with other peers succeeded")
	}
	if _, err := w.Load(); err != nil {
		t.Errorf("Load of a bound file: %v", err)
	}

	unbound := filepath.Join(t.TempDir(), "unbound.wal")
	u := openT(t, unbound)
	if err := u.SaveMaxRound(1); err != nil {
		t.Fatal(err)
	}
	if err := u.Bind(1, []paxos.NodeID{1}); err == nil {
		t.Error("Bind on a file with records but no membership succeeded")
	}
}

// TestNodeRestartsFromFile runs a single-replica log on the file store,
// chooses values, closes the file as a killed process would leave it, and
// requires a node rebuilt from the reopened file to hold the same promise,
// round and chosen entries, and to keep choosing above them.
func TestNodeRestartsFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	cfg := replog.DefaultConfig(1, []paxos.NodeID{1})
	start := func() (*File, *replog.Node, time.Duration) {
		w := openT(t, path)
		if err := w.Bind(1, cfg.Peers); err != nil {
			t.Fatal(err)
		}
		n, err := replog.New(cfg, w, rand.New(rand.NewPCG(1, 0)))
		if err != nil {
			t.Fatal(err)
		}
		var now time.Duration
		for i := 0; i < 100 && !n.Ready(); i++ {
			now += 25 * time.Millisecond
			n.Tick(now)
		}
		if !n.Ready() {
			t.Fatal("single node never became a ready leader")
		}
		return w, n, now
	}
	w, n, now := start()
	for _, v := range []string{"a", "b", "c"} {
		if _, err := n.Propose(now, paxos.Value(v)); err != nil {
			t.Fatal(err)
		}
	}
	commit, round, promised := n.CommitIndex(), n.MaxRound(), n.Promised()
	chosen := map[paxos.Slot]string{}
	for s := paxos.Slot(1); s <= commit; s++ {
		e, _ := n.Chosen(s)
		chosen[s] = string(e.Value)
	}
	w.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := replog.New(cfg, w2, rand.New(rand.NewPCG(2, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if restored.CommitIndex() != commit || restored.MaxRound() != round || restored.Promised() != promised {
		t.Fatalf("restored commit %d round %d promised %v, want %d %d %v",
			restored.CommitIndex(), restored.MaxRound(), restored.Promised(), commit, round, promised)
	}
	for s, v := range chosen {
		if e, ok := restored.Chosen(s); !ok || string(e.Value) != v {
			t.Errorf("slot %d = %+v ok=%t, want %q", s, e, ok, v)
		}
	}
	w2.Close()

	_, again, now := start()
	if again.MaxRound() <= round {
		t.Errorf("the next election used round %d, not above the saved %d", again.MaxRound(), round)
	}
	if _, err := again.Propose(now, paxos.Value("d")); err != nil {
		t.Fatal(err)
	}
	if e, ok := again.Chosen(again.CommitIndex()); !ok || string(e.Value) != "d" || again.CommitIndex() <= commit {
		t.Fatalf("after restart the last chosen entry is %+v at %d, want d above %d", e, again.CommitIndex(), commit)
	}
}
