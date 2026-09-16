// Command chaos runs the deterministic simulation from the command line: one
// seed or a sweep of seeds, with the random fault schedule or a named
// scenario. On an invariant violation it prints the seed, the step and the
// exact command line that replays the run, and exits 1.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/oguzhanozfe/paxos-arena/internal/sim"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "chaos:", err)
		}
		os.Exit(1)
	}
}

// run parses args, runs every seed of the sweep and writes one report line
// per seed to stdout. It returns the first violation, the first liveness
// failure, or ctx.Err() when interrupted.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seed := fs.Uint64("seed", 1, "first seed")
	seeds := fs.Int("seeds", 1, "number of seeds to sweep, seed..seed+N-1")
	nodes := fs.Int("nodes", 3, "cluster size")
	steps := fs.Int("steps", 6000, "events in the fault phase")
	liveness := fs.Int("liveness-steps", 40000, "event bound for the liveness phase after healing")
	drop := fs.Float64("drop", 0.1, "message loss probability")
	dup := fs.Float64("dup", 0.05, "message duplication probability")
	scenario := fs.String("scenario", "", "scripted scenario name; empty for the random schedule")
	tracePath := fs.String("trace", "", "write the event trace to this file (suffixed with the seed in a sweep)")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	list := fs.Bool("list-scenarios", false, "print the scenario names and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *list {
		for _, name := range sim.Scenarios() {
			fmt.Fprintln(stdout, name)
		}
		return nil
	}
	if *seeds < 1 {
		return errors.New("-seeds must be at least 1")
	}
	var level slog.LevelVar
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: &level}))

	for s := *seed; s < *seed+uint64(*seeds); s++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := sim.DefaultParams(s)
		p.Nodes = *nodes
		p.Steps = *steps
		p.LivenessSteps = *liveness
		p.Faults.DropP = *drop
		p.Faults.DupP = *dup
		p.Scenario = *scenario
		if err := runSeed(ctx, p, *tracePath, *seeds > 1, stdout, logger); err != nil {
			return err
		}
	}
	return nil
}

// runSeed runs one seed: fault phase, heal, liveness phase, report.
func runSeed(ctx context.Context, p sim.Params, tracePath string, sweep bool, stdout io.Writer, logger *slog.Logger) error {
	var trace io.Writer
	if tracePath != "" {
		path := tracePath
		if sweep {
			path = fmt.Sprintf("%s.%d", tracePath, p.Seed)
		}
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create trace: %w", err)
		}
		w := bufio.NewWriterSize(f, 1<<20)
		defer func() {
			if err := w.Flush(); err != nil {
				logger.Error("flush trace", "path", path, "err", err)
			}
			if err := f.Close(); err != nil {
				logger.Error("close trace", "path", path, "err", err)
			}
		}()
		trace = w
	}
	r, err := sim.New(p, trace)
	if err != nil {
		return err
	}
	ep := r.Params()
	logger.InfoContext(ctx, "run start", "seed", ep.Seed, "nodes", ep.Nodes, "steps", ep.Steps,
		"scenario", ep.Scenario, "lease", !ep.NoLease)
	err = r.RunFaultsContext(ctx)
	if err == nil {
		r.Heal()
		err = r.RunLivenessContext(ctx)
	}
	rep := r.Report()
	if err != nil {
		if ctx.Err() != nil {
			logger.WarnContext(ctx, "interrupted", "seed", ep.Seed, "step", r.Steps())
			return ctx.Err()
		}
		fmt.Fprintf(stdout, "seed=%d step=%d scenario=%q\n%v\nreplay: %s\n", ep.Seed, r.Steps(), ep.Scenario, err, replayCommand(ep))
		return err
	}
	fmt.Fprintln(stdout, formatReport(ep, rep))
	logger.InfoContext(ctx, "run done", "seed", ep.Seed, "steps", rep.Steps, "sim_time", rep.SimTime,
		"elections", rep.Elections, "crashes", rep.Crashes, "partitions", rep.Partitions)
	return nil
}

func formatReport(p sim.Params, rep sim.Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d scenario=%q nodes=%d lease=%t steps=%d sim_time=%v", p.Seed, p.Scenario, rep.Nodes,
		rep.LeaseEnabled, rep.Steps, rep.SimTime)
	fmt.Fprintf(&b, " elections=%d leader_changes=%d crashes=%d torn_writes=%d partitions=%d", rep.Elections,
		rep.LeaderChanges, rep.Crashes, rep.TornWrites, rep.Partitions)
	fmt.Fprintf(&b, " net{sent=%d delivered=%d dropped=%d duplicated=%d blocked=%d}", rep.Net.Sent, rep.Net.Delivered,
		rep.Net.Dropped, rep.Net.Duplicated, rep.Net.Blocked)
	fmt.Fprintf(&b, " submits=%d completed=%d reads=%d reads_ok=%d reads_failed=%d", rep.Submits, rep.Completed,
		rep.Reads, rep.ReadsCompleted, rep.ReadsFailed)
	fmt.Fprintf(&b, " commit=%d applied=%d participants=%d", rep.Commit, rep.Applied, rep.Participants)
	keys := make([]string, 0, len(rep.Marks))
	for k := range rep.Marks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%d", k, rep.Marks[k])
	}
	return b.String()
}

func replayCommand(p sim.Params) string {
	return fmt.Sprintf("go run ./cmd/chaos -seed %d -nodes %d -steps %d -liveness-steps %d -drop %g -dup %g -scenario %q",
		p.Seed, p.Nodes, p.Steps, p.LivenessSteps, p.Faults.DropP, p.Faults.DupP, p.Scenario)
}
