package sim

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

var (
	seedsFlag       = flag.Int("seeds", 50, "seeds per scenario in TestScenarios")
	randomSeedsFlag = flag.Int("random-seeds", 200, "seeds in TestRandom")
)

func scenarioSeeds() int {
	if testing.Short() {
		return min(*seedsFlag, 5)
	}
	return *seedsFlag
}

func randomSeeds() int {
	if testing.Short() {
		return min(*randomSeedsFlag, 20)
	}
	return *randomSeedsFlag
}

// replay is the command line that reproduces a run.
func replay(p Params) string {
	return fmt.Sprintf("go run ./cmd/chaos -seed %d -nodes %d -steps %d -drop %g -dup %g -scenario %q",
		p.Seed, p.Nodes, p.Steps, p.Faults.DropP, p.Faults.DupP, p.Scenario)
}

// runFull runs the fault phase, heals and runs the liveness phase, failing
// the test with the replay command on any error.
func runFull(t *testing.T, p Params) Report {
	t.Helper()
	r, err := New(p, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.RunFaults(); err != nil {
		t.Fatalf("fault phase: %v\nreplay: %s", err, replay(r.Params()))
	}
	r.Heal()
	if err := r.RunLiveness(); err != nil {
		t.Fatalf("liveness phase: %v\nreplay: %s", err, replay(r.Params()))
	}
	rep := r.Report()
	if rep.Participants != rep.Nodes {
		t.Fatalf("%d of %d nodes participated in the final round\nreplay: %s", rep.Participants, rep.Nodes, replay(r.Params()))
	}
	minIssued := r.Params().Clients * r.Params().commandsPerClient()
	if rep.Issued < minIssued {
		t.Fatalf("issued %d client commands, want at least %d\nreplay: %s", rep.Issued, minIssued, replay(r.Params()))
	}
	if rep.Completed != rep.Issued {
		t.Fatalf("completed %d of %d issued client commands\nreplay: %s", rep.Completed, rep.Issued, replay(r.Params()))
	}
	return rep
}

// checkScenario asserts what each scenario of design section 7 must show
// beyond the invariants the checker enforces.
func checkScenario(t *testing.T, name string, rep Report) {
	t.Helper()
	switch name {
	case "dueling_leaders":
		if rep.Partitions < 1 {
			t.Errorf("no duel was staged")
		}
		if rep.LeaseEnabled && rep.Elections > 3*rep.Partitions+3 {
			t.Errorf("with the lease enabled, %d elections for %d partitions exceeds the bound 3p+3", rep.Elections, rep.Partitions)
		}
	case "partition_and_heal":
		if rep.Partitions < 1 {
			t.Errorf("no partition was staged")
		}
		if rep.Reads == 0 {
			t.Errorf("no consistent reads were issued")
		}
	case "duplicated_and_reordered_messages":
		if rep.Net.Duplicated == 0 {
			t.Errorf("no message was duplicated")
		}
	case "crash_restart_storm":
		if rep.Crashes < 1 {
			t.Errorf("no node crashed")
		}
	case "clock_skew":
		t.Logf("clock skew: %d elections, %d leader changes", rep.Elections, rep.LeaderChanges)
	case "late_learner":
		if rep.Marks["late_learner.unblocked_at_commit"] == 0 {
			t.Errorf("the late learner never missed %d slots: marks %v", lateLearnerSlots, rep.Marks)
		}
	}
	if rep.ReadsCompleted == 0 {
		t.Errorf("no consistent read completed")
	}
}

func TestScenarios(t *testing.T) {
	for _, name := range Scenarios() {
		t.Run(name, func(t *testing.T) {
			for seed := 1; seed <= scenarioSeeds(); seed++ {
				t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
					t.Parallel()
					p := DefaultParams(uint64(seed))
					p.Scenario = name
					rep := runFull(t, p)
					checkScenario(t, name, rep)
				})
			}
		})
	}
}

func TestRandom(t *testing.T) {
	for seed := 1; seed <= randomSeeds(); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runFull(t, DefaultParams(uint64(seed)))
		})
	}
}

// TestLivenessAfterHeal states its fairness assumption in its name: after
// Heal stops every fault, every client command is chosen and every node
// applies through one commit index within the liveness bound.
func TestLivenessAfterHeal(t *testing.T) {
	for seed := 1; seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(uint64(seed))
			p.Nodes = 3 + 2*(seed%2)
			p.Faults.DropP = 0.25
			p.PartitionP = 0.02
			p.CrashP = 0.02
			p.TornWriteP = 0.3
			rep := runFull(t, p)
			if rep.Crashes == 0 && rep.Partitions == 0 {
				t.Errorf("the fault phase injected nothing")
			}
		})
	}
}

// TestLivenessWithFrozenMinority heals only a majority core and freezes
// every fault outside it, then requires progress from the core alone with
// every core member participating.
func TestLivenessWithFrozenMinority(t *testing.T) {
	for seed := 1; seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(uint64(seed))
			p.Nodes = 5
			p.CrashP = 0.02
			p.PartitionP = 0.02
			r, err := New(p, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.RunFaults(); err != nil {
				t.Fatalf("fault phase: %v\nreplay: %s", err, replay(r.Params()))
			}
			core := []paxos.NodeID{paxos.NodeID(1 + seed%5), paxos.NodeID(1 + (seed+1)%5), paxos.NodeID(1 + (seed+2)%5)}
			r.HealCore(core)
			if err := r.RunLiveness(); err != nil {
				t.Fatalf("liveness phase with core %v: %v\nreplay: %s", core, err, replay(r.Params()))
			}
			rep := r.Report()
			if rep.Participants != len(core) {
				t.Fatalf("%d of %d core nodes participated", rep.Participants, len(core))
			}
		})
	}
}

// TestCheckerDetectsKnownBugs turns each unsafe knob on in turn and requires
// the checker to report the invariant the knob breaks within 200 seeds of
// TestRandom parameters. A knob the checker misses is a checker bug.
func TestCheckerDetectsKnownBugs(t *testing.T) {
	cases := []struct {
		name string
		knob replog.UnsafeKnobs
		want string
	}{
		{"AcceptBelowPromise", replog.UnsafeKnobs{AcceptBelowPromise: true}, "S3"},
		{"IgnorePhase1Reports", replog.UnsafeKnobs{IgnorePhase1Reports: true}, "S1"},
		{"SkipLeadershipNoOp", replog.UnsafeKnobs{SkipLeadershipNoOp: true}, "S7"},
		{"ForgetAcceptedOnRestart", replog.UnsafeKnobs{ForgetAcceptedOnRestart: true}, "S5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			others := map[string]int{}
			for seed := 1; seed <= 200; seed++ {
				p := DefaultParams(uint64(seed))
				knob := tc.knob
				p.unsafe = &knob
				r, err := New(p, nil)
				if err != nil {
					t.Fatal(err)
				}
				err = r.RunFaults()
				if err == nil {
					r.Heal()
					err = r.RunLiveness()
				}
				var v *Violation
				if errors.As(err, &v) {
					if v.Invariant == tc.want {
						t.Logf("knob %s caught as %s at seed %d step %d: %s", tc.name, v.Invariant, seed, v.Step, v.Detail)
						return
					}
					others[v.Invariant]++
				}
			}
			t.Fatalf("knob %s never reported as %s in 200 seeds (other invariants reported: %v)", tc.name, tc.want, others)
		})
	}
}

// TestSeedCorpus reruns every seed in testdata/seeds.txt. Each line is
// "<seed>" or "<seed> <scenario>"; blank lines and # comments are skipped.
func TestSeedCorpus(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "seeds.txt")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open seed corpus: %v", err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		seed, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			t.Fatalf("seed corpus line %q: %v", line, err)
		}
		scenario := ""
		if len(fields) > 1 {
			scenario = fields[1]
		}
		n++
		t.Run(line, func(t *testing.T) {
			p := DefaultParams(seed)
			p.Scenario = scenario
			runFull(t, p)
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d seeds in the corpus", n)
}

func TestReplayDeterministic(t *testing.T) {
	run := func() ([]byte, Report) {
		var buf bytes.Buffer
		p := DefaultParams(11)
		p.Steps = 3000
		r, err := New(p, &buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.RunFaults(); err != nil {
			t.Fatalf("%v\nreplay: %s", err, replay(p))
		}
		r.Heal()
		if err := r.RunLiveness(); err != nil {
			t.Fatalf("%v\nreplay: %s", err, replay(p))
		}
		return buf.Bytes(), r.Report()
	}
	t1, r1 := run()
	t2, r2 := run()
	if !bytes.Equal(t1, t2) {
		t.Fatal("two runs with the same seed produced different traces")
	}
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("two runs with the same seed produced different reports:\n%+v\n%+v", r1, r2)
	}
	if len(t1) == 0 {
		t.Fatal("trace is empty")
	}
}

func TestParamsValidate(t *testing.T) {
	cases := []struct {
		name    string
		mod     func(*Params)
		wantErr bool
	}{
		{"defaults", func(*Params) {}, false},
		{"one node", func(p *Params) { p.Nodes = 1 }, false},
		{"zero nodes", func(p *Params) { p.Nodes = 0 }, true},
		{"negative steps", func(p *Params) { p.Steps = -1 }, true},
		{"drop above one", func(p *Params) { p.Faults.DropP = 1.5 }, true},
		{"negative crash", func(p *Params) { p.CrashP = -0.1 }, true},
		{"max delay below min", func(p *Params) { p.Faults.MinDelay = time.Second }, true},
		{"restart bounds reversed", func(p *Params) { p.RestartAfter = [2]time.Duration{time.Second, 0} }, true},
		{"negative skew", func(p *Params) { p.ClockSkewMax = -time.Second }, true},
		{"negative clients", func(p *Params) { p.Clients = -1 }, true},
		{"unknown scenario", func(p *Params) { p.Scenario = "nope" }, true},
		{"known scenario", func(p *Params) { p.Scenario = "late_learner" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := DefaultParams(1)
			tc.mod(&p)
			err := p.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %t", err, tc.wantErr)
			}
			if _, err := New(p, nil); (err != nil) != tc.wantErr {
				t.Fatalf("New() = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

func TestScenariosListed(t *testing.T) {
	want := []string{"dueling_leaders", "partition_and_heal", "duplicated_and_reordered_messages",
		"crash_restart_storm", "clock_skew", "late_learner"}
	if got := Scenarios(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Scenarios() = %v, want %v", got, want)
	}
	if findScenario("nope") != nil {
		t.Error("findScenario returned a scenario for an unknown name")
	}
}

func TestSingleNodeRun(t *testing.T) {
	p := DefaultParams(3)
	p.Nodes = 1
	p.CrashP = 0.01
	runFull(t, p)
}

func TestStepAfterViolationRepeatsIt(t *testing.T) {
	p := DefaultParams(1)
	knob := replog.UnsafeKnobs{AcceptBelowPromise: true}
	p.unsafe = &knob
	p.Steps = 200000
	r, err := New(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = r.RunFaults()
	var v *Violation
	if !errors.As(err, &v) {
		t.Skipf("seed 1 did not trigger the knob within the fault phase: %v", err)
	}
	if err2 := r.Step(); err2 != err {
		t.Fatalf("Step after a violation returned %v, want the same violation %v", err2, err)
	}
	if r.Violation() != err {
		t.Fatal("Violation() does not return the recorded violation")
	}
}

// TestNoGoroutineLeak checks that a run starts no goroutines that outlive
// it: the simulator is single-threaded by design.
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	p := DefaultParams(5)
	p.Steps = 2000
	runFull(t, p)
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines: %d before, %d after", before, after)
	}
}

// FuzzSeed treats the input as a run seed for a short random run and fails
// only on a safety violation; a liveness bound reached in a short run is
// not a failure.
func FuzzSeed(f *testing.F) {
	f.Add(uint64(1))
	f.Add(uint64(42))
	f.Fuzz(func(t *testing.T, seed uint64) {
		p := DefaultParams(seed)
		p.Steps = 1500
		p.LivenessSteps = 15000
		r, err := New(p, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = r.RunFaults()
		if err == nil {
			r.Heal()
			err = r.RunLiveness()
		}
		var v *Violation
		if errors.As(err, &v) {
			t.Fatalf("%v\nreplay: %s", err, replay(p))
		}
	})
}
