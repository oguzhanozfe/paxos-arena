package sim

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// runTraced runs p to the end and returns the trace digest, the report and
// the error text.
func runTraced(p Params) ([32]byte, Report, string) {
	var buf bytes.Buffer
	r, err := New(p, &buf)
	if err != nil {
		return [32]byte{}, Report{}, "new: " + err.Error()
	}
	err = r.RunFaults()
	if err == nil {
		r.Heal()
		err = r.RunLiveness()
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return sha256.Sum256(buf.Bytes()), r.Report(), msg
}

// TestReviewEveryScenarioReplaysIdentically runs every scenario, the random
// schedule and every unsafe knob twice per seed and requires identical
// traces, reports and error texts.
func TestReviewEveryScenarioReplaysIdentically(t *testing.T) {
	if os.Getenv("ARENA_REVIEW_REPRO") == "" {
		t.Skip("determinism sweep; set ARENA_REVIEW_REPRO=1")
	}
	type tc struct {
		name string
		p    Params
	}
	var cases []tc
	for _, sc := range append([]string{""}, Scenarios()...) {
		for seed := uint64(1); seed <= 3; seed++ {
			p := DefaultParams(seed)
			p.Scenario = sc
			cases = append(cases, tc{fmt.Sprintf("scenario=%q/seed=%d", sc, seed), p})
		}
	}
	knobs := map[string]replog.UnsafeKnobs{
		"AcceptBelowPromise":      {AcceptBelowPromise: true},
		"IgnorePhase1Reports":     {IgnorePhase1Reports: true},
		"SkipLeadershipNoOp":      {SkipLeadershipNoOp: true},
		"ForgetAcceptedOnRestart": {ForgetAcceptedOnRestart: true},
	}
	for name, knob := range knobs {
		for seed := uint64(1); seed <= 25; seed++ {
			p := DefaultParams(seed)
			k := knob
			p.unsafe = &k
			cases = append(cases, tc{fmt.Sprintf("knob=%s/seed=%d", name, seed), p})
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h1, r1, e1 := runTraced(c.p)
			h2, r2, e2 := runTraced(c.p)
			if h1 != h2 {
				t.Errorf("traces differ")
			}
			if !reflect.DeepEqual(r1, r2) {
				t.Errorf("reports differ:\n%+v\n%+v", r1, r2)
			}
			if e1 != e2 {
				t.Errorf("errors differ:\n%s\n%s", e1, e2)
			}
		})
	}
}

// TestReviewParamsValidateNamesTheSameField: Validate ranged over a map
// literal, so with several out-of-range probabilities the field it named
// depended on map iteration order. It now checks the fields in a fixed
// order, so this runs by default as a regression test and also pins the
// first field reported.
func TestReviewParamsValidateNamesTheSameField(t *testing.T) {
	p := DefaultParams(1)
	p.Faults.DropP, p.Faults.DupP, p.CrashP = 2, 3, 4
	seen := map[string]int{}
	for i := 0; i < 200; i++ {
		seen[p.Validate().Error()]++
	}
	if len(seen) > 1 {
		t.Fatalf("Validate returned %d different errors for the same Params: %v", len(seen), seen)
	}
	if want := "sim: DropP = 2 is not a probability"; seen[want] != 200 {
		t.Fatalf("Validate errors = %v, want %q every time", seen, want)
	}
}
