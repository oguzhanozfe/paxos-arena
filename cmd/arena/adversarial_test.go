package main

// Adversarial test of the per-process deployment: every replica is started
// through run with -id and -peers, exactly as `arena -id <n> -peers ...`
// would be, and stopped by cancelling its context, as a killed process is.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/api"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// advReplica is one `arena -id -peers` instance running in this process.
type advReplica struct {
	id     paxos.NodeID
	addr   string
	url    string
	cancel context.CancelFunc
	errc   chan error
}

func advFreeAddrs(t *testing.T, n int) []string {
	t.Helper()
	var lns []net.Listener
	var out []string
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		out = append(out, ln.Addr().String())
	}
	for _, ln := range lns {
		ln.Close()
	}
	return out
}

// advGet performs a GET and never fails the test: a stopped or starting
// replica refuses connections.
func advGet(url string, out any) int {
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil {
		json.Unmarshal(b, out)
	}
	return resp.StatusCode
}

// advStartReplica runs one replica through run, with its log file in dir,
// and waits until it serves. The -wal flag was added by the fix: per-process
// mode now refuses to start without a durable store, so the test passes the
// same file on every start of a replica, exactly as an operator restarting
// a killed process would.
func advStartReplica(t *testing.T, id paxos.NodeID, addr, peers, dir string) *advReplica {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := &advReplica{id: id, addr: addr, url: "http://" + addr, cancel: cancel, errc: make(chan error, 1)}
	args := []string{"-id", fmt.Sprint(id), "-listen", addr, "-peers", peers, "-log-level", "error",
		"-wal", filepath.Join(dir, fmt.Sprintf("node%d.wal", id))}
	go func() { p.errc <- run(ctx, args, io.Discard, io.Discard) }()
	deadline := time.Now().Add(10 * time.Second)
	for advGet(p.url+"/healthz", nil) != http.StatusOK {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("replica %d did not start on %s", id, addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(p.stop)
	return p
}

func (p *advReplica) stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	p.cancel = nil
	select {
	case <-p.errc:
	case <-time.After(10 * time.Second):
	}
}

func advWaitLeader(t *testing.T, reps ...*advReplica) *advReplica {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range reps {
			var st api.NodeResponse
			if advGet(p.url+"/v1/node", &st) == http.StatusOK && st.Role == replog.Leader && st.Ready {
				return p
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no ready leader within 15s")
	return nil
}

// TestAttackRestartedReplicaForgetsAcknowledgedCreate: the per-process mode
// is advertised for killing and restarting one replica independently, but
// runSingle used to open an empty replog.MemStore, so a restarted process
// rejoined under its old identity with no promise and no accepted values
// (DESIGN section 9 forbids exactly that). It now requires -wal and restarts
// from the file. Execution on a three-replica
// cluster: replicas 1 and 2 are up, replica 3 is not yet started; a
// tournament is created and acknowledged with 201 and applied on both; the
// leader's process dies; replica 2's process is restarted; replica 3 starts.
// With a durable store replica 2 would still hold the chosen create and the
// majority {2, 3} would serve it. The tournament an operator was told exists
// must still exist on a consistent read.
func TestAttackRestartedReplicaForgetsAcknowledgedCreate(t *testing.T) {
	addrs := advFreeAddrs(t, 3)
	var parts []string
	for i, a := range addrs {
		parts = append(parts, fmt.Sprintf("%d=http://%s", i+1, a))
	}
	peers := strings.Join(parts, ",")
	dir := t.TempDir()
	r1 := advStartReplica(t, 1, addrs[0], peers, dir)
	r2 := advStartReplica(t, 2, addrs[1], peers, dir)
	leader := advWaitLeader(t, r1, r2)
	body := `{"id":"t-acked","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[10000],"min_entrants":1,"max_entrants":10,"max_score":1000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":1,"jurisdictions":[]}}}`
	var created api.CommandResponse
	if code, _ := call(t, "POST", leader.url+"/v1/tournaments", "create-acked", body, &created); code != http.StatusCreated {
		t.Fatalf("setup: create answered %d: %+v", code, created)
	}
	// Both live replicas apply it, so the value is chosen and held by a
	// majority of the three.
	for _, p := range []*advReplica{r1, r2} {
		deadline := time.Now().Add(5 * time.Second)
		for advGet(p.url+"/v1/tournaments/t-acked?read=stale", nil) != http.StatusOK {
			if time.Now().After(deadline) {
				t.Fatalf("setup: replica %d never applied the create", p.id)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Logf("create acknowledged at slot %d by leader %d and applied on replicas 1 and 2", created.Slot, leader.id)

	r1.stop()
	r2.stop()
	r2 = advStartReplica(t, 2, addrs[1], peers, dir) // the documented restart of one process
	r3 := advStartReplica(t, 3, addrs[2], peers, dir)
	newLeader := advWaitLeader(t, r2, r3)

	var got api.TournamentResponse
	code := advGet(newLeader.url+"/v1/tournaments/t-acked", &got)
	if code != http.StatusOK {
		// Show that the identifier is now free: a different tournament can
		// be created under it, so two records exist for one id.
		var again api.CommandResponse
		recreate, _ := call(t, "POST", newLeader.url+"/v1/tournaments", "create-again",
			strings.Replace(body, `"entry_fee":500`, `"entry_fee":900`, 1), &again)
		t.Fatalf("consistent read of the acknowledged tournament on the new leader %d answered %d (want 200); "+
			"re-creating the same id with other rules answered %d: the acknowledged create was lost when replica 2's process restarted with an empty store",
			newLeader.id, code, recreate)
	}
}
