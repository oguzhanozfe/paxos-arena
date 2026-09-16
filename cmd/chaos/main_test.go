package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRunPrintsReport(t *testing.T) {
	before := runtime.NumGoroutine()
	var stdout, stderr bytes.Buffer
	args := []string{"-seed", "1", "-seeds", "2", "-nodes", "3", "-steps", "800", "-liveness-steps", "30000", "-log-level", "warn"}
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
		for _, field := range []string{"steps=", "elections=", "net{sent=", "completed=", "participants="} {
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
	info, err := os.Stat(tracePath)
	if err != nil {
		t.Fatalf("trace file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("trace file is empty")
	}
}

func TestRunListScenarios(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(t.Context(), []string{"-list-scenarios"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dueling_leaders", "late_learner"} {
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
