package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRunPrintsReport(t *testing.T) {
	before := runtime.NumGoroutine()
	var stdout, stderr bytes.Buffer
	args := []string{"-seed", "1", "-seeds", "2", "-nodes", "3", "-steps", "800", "-liveness-steps", "60000", "-log-level", "warn", "-no-summary"}
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d report lines, want 2:\n%s", len(lines), stdout.String())
	}
	for i, want := range []string{"seed=1 ", "seed=2 "} {
		if !strings.HasPrefix(lines[i], want) {
			t.Errorf("line %d = %q, want prefix %q", i, lines[i], want)
		}
		for _, field := range []string{"steps=", "elections=", "net{sent=", "completed=", "settled=", "keys=", "replays=", "participants="} {
			if !strings.Contains(lines[i], field) {
				t.Errorf("line %d lacks %q: %s", i, field, lines[i])
			}
		}
	}
	if strings.Contains(stderr.String(), "level=INFO") {
		t.Errorf("-log-level warn still printed INFO records:\n%s", stderr.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutines: %d before, %d after run", before, after)
	}
}

// TestRunPrintsSummary checks the per-scenario table and the invariant
// table that follow the report lines.
func TestRunPrintsSummary(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"-seed", "3", "-seeds", "2", "-steps", "800", "-log-level", "error"}
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v\nstdout:\n%s", err, stdout.String())
	}
	out := stdout.String()
	for _, want := range []string{"scenario  runs", "invariant  checks  result", "S1  ", "S8  ", "D1  ", "D6  ", "  ok  "} {
		if !strings.Contains(out, want) {
			t.Errorf("summary lacks %q:\n%s", want, out)
		}
	}
	if !regexp.MustCompile(`(?m)^random\s+2\s+\d+.*\sok$`).MatchString(out) {
		t.Errorf("summary lacks the random row with 2 runs:\n%s", out)
	}
	if strings.Contains(out, "not evaluated") {
		t.Errorf("some invariant was not evaluated:\n%s", out)
	}
	if regexp.MustCompile(`(?m)^P[1-6]\s`).MatchString(out) {
		t.Errorf("the random schedule sends no play commands, but the summary lists play invariants:\n%s", out)
	}
}

func TestRunScenarioWithTrace(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.txt")
	var stdout, stderr bytes.Buffer
	args := []string{"-seed", "2", "-scenario", "crash_restart_storm", "-steps", "1000", "-trace", tracePath, "-log-level", "error"}
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), `scenario="crash_restart_storm"`) {
		t.Errorf("report lacks the scenario name: %s", stdout.String())
	}
	if !hasSummaryRow(stdout.String(), "crash_restart_storm") {
		t.Errorf("summary lacks the scenario row: %s", stdout.String())
	}
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatalf("trace file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("trace file is empty")
	}
}

func TestRunAllScenarios(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"-seed", "1", "-scenario", "all", "-steps", "1000", "-log-level", "error"}
	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	for _, name := range []string{"leader_crash_mid_settlement", "client_retry_storm", "exclusion_change_at_settle", "late_learner",
		"duplicate_intents_after_leader_change", "stale_sequence_replay", "partition_during_payout_claim",
		"token_expiry_mid_round", "deal_during_leader_change"} {
		if !hasSummaryRow(stdout.String(), name) {
			t.Errorf("summary lacks a row for %s:\n%s", name, stdout.String())
		}
	}
	for _, id := range []string{"P1", "P2", "P3", "P4", "P5", "P6"} {
		if !regexp.MustCompile(`(?m)^` + id + `\s+[1-9]\d*\s+ok\s`).MatchString(stdout.String()) {
			t.Errorf("summary lacks an evaluated %s row:\n%s", id, stdout.String())
		}
	}
}

// hasSummaryRow reports whether the summary table has a row for the
// scenario with one run and an ok result.
func hasSummaryRow(out, scenario string) bool {
	return regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(scenario) + `\s+1\s+\d+.*\sok$`).MatchString(out)
}

func TestRunListScenarios(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(t.Context(), []string{"-list-scenarios"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dueling_leaders", "late_learner", "leader_crash_mid_settlement", "client_retry_storm", "exclusion_change_at_settle",
		"duplicate_intents_after_leader_change", "stale_sequence_replay", "partition_during_payout_claim", "token_expiry_mid_round", "deal_during_leader_change"} {
		if !strings.Contains(stdout.String(), name+"\n") {
			t.Errorf("scenario list lacks %s:\n%s", name, stdout.String())
		}
	}
}

func TestRunRejectsBadArguments(t *testing.T) {
	cases := [][]string{
		{"-bogus"},
		{"-seeds", "0"},
		{"-scenario", "nope"},
		{"-log-level", "loud"},
		{"-nodes", "0"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		if err := run(t.Context(), args, &stdout, &stderr); err == nil {
			t.Errorf("run(%v) returned no error", args)
		}
	}
}

func TestRunStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	err := run(ctx, []string{"-seed", "1", "-steps", "100000", "-log-level", "error"}, &stdout, &stderr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run with a cancelled context returned %v, want context.Canceled", err)
	}
	if stdout.Len() != 0 {
		t.Errorf("a cancelled run printed a report: %s", stdout.String())
	}
}
