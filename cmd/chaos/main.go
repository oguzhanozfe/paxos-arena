// Command chaos runs the deterministic simulation from the command line: one
// seed or a sweep of seeds, with the random fault schedule, a named scenario
// or every scenario. It prints one report line per run, then a summary
// table per scenario and the result of every invariant the checker
// evaluated. On an invariant violation it prints the seed, the step and the
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
	"text/tabwriter"

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

// summary aggregates the reports of one scenario.
type summary struct {
	scenario   string
	runs       int
	steps      int
	elections  int
	crashes    int
	partitions int
	settled    int
	keys       int
	replays    int
	keyReused  int
	checks     map[string]int
}

// run parses args, runs every seed of the sweep for every selected scenario
// and writes one report line per run to stdout, followed by the summary
// tables. It returns the first violation, the first liveness failure, or
// ctx.Err() when interrupted.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seed := fs.Uint64("seed", 1, "first seed")
	seeds := fs.Int("seeds", 1, "number of seeds to sweep, seed..seed+N-1")
	nodes := fs.Int("nodes", 3, "cluster size (a scenario may override it)")
	steps := fs.Int("steps", 6000, "events in the fault phase (a scenario may raise it)")
	liveness := fs.Int("liveness-steps", 60000, "event bound for the liveness phase after healing")
	drop := fs.Float64("drop", 0.1, "message loss probability")
	dup := fs.Float64("dup", 0.05, "message duplication probability")
	scenario := fs.String("scenario", "", "scripted scenario name, \"all\" for every scenario, empty for the random schedule")
	tracePath := fs.String("trace", "", "write the event trace to this file (suffixed with the scenario and seed in a sweep)")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	noSummary := fs.Bool("no-summary", false, "do not print the summary tables")
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

	var names []string
	switch *scenario {
	case "all":
		names = sim.Scenarios()
	default:
		names = []string{*scenario}
	}
	sweep := *seeds > 1 || len(names) > 1
	var summaries []*summary
	for _, name := range names {
		sum := &summary{scenario: name, checks: make(map[string]int)}
		summaries = append(summaries, sum)
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
			p.Scenario = name
			rep, err := runSeed(ctx, p, *tracePath, sweep, stdout, logger)
			if err != nil {
				return err
			}
			sum.add(rep)
		}
	}
	if !*noSummary {
		printSummary(stdout, summaries)
	}
	return nil
}

func (s *summary) add(rep sim.Report) {
	s.runs++
	s.steps += rep.Steps
	s.elections += rep.Elections
	s.crashes += rep.Crashes
	s.partitions += rep.Partitions
	s.settled += rep.Settled
	s.keys += rep.Keys
	s.replays += rep.Replays
	s.keyReused += rep.KeyReused
	for k, v := range rep.Checks {
		s.checks[k] += v
	}
}

// runSeed runs one seed: fault phase, heal, liveness phase, report line.
func runSeed(ctx context.Context, p sim.Params, tracePath string, sweep bool, stdout io.Writer, logger *slog.Logger) (sim.Report, error) {
	var trace io.Writer
	if tracePath != "" {
		path := tracePath
		if sweep {
			path = fmt.Sprintf("%s.%s.%d", tracePath, scenarioLabel(p.Scenario), p.Seed)
		}
		f, err := os.Create(path)
		if err != nil {
			return sim.Report{}, fmt.Errorf("create trace: %w", err)
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
		return sim.Report{}, err
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
			return rep, ctx.Err()
		}
		fmt.Fprintf(stdout, "seed=%d step=%d scenario=%q\n%v\nreplay: %s\n", ep.Seed, r.Steps(), ep.Scenario, err, replayCommand(ep))
		return rep, err
	}
	fmt.Fprintln(stdout, formatReport(ep, rep))
	logger.InfoContext(ctx, "run done", "seed", ep.Seed, "steps", rep.Steps, "sim_time", rep.SimTime,
		"elections", rep.Elections, "crashes", rep.Crashes, "partitions", rep.Partitions, "settled", rep.Settled)
	return rep, nil
}

func scenarioLabel(name string) string {
	if name == "" {
		return "random"
	}
	return name
}

func formatReport(p sim.Params, rep sim.Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d scenario=%q nodes=%d lease=%t steps=%d sim_time=%v", p.Seed, p.Scenario, rep.Nodes,
		rep.LeaseEnabled, rep.Steps, rep.SimTime)
	fmt.Fprintf(&b, " elections=%d leader_changes=%d crashes=%d torn_writes=%d partitions=%d", rep.Elections,
		rep.LeaderChanges, rep.Crashes, rep.TornWrites, rep.Partitions)
	fmt.Fprintf(&b, " net{sent=%d delivered=%d dropped=%d duplicated=%d blocked=%d}", rep.Net.Sent, rep.Net.Delivered,
		rep.Net.Dropped, rep.Net.Duplicated, rep.Net.Blocked)
	fmt.Fprintf(&b, " issued=%d completed=%d submits=%d extra_retries=%d mutated=%d unexpected=%d", rep.Issued, rep.Completed,
		rep.Submits, rep.ExtraRetries, rep.Mutated, rep.Unexpected)
	fmt.Fprintf(&b, " settled=%d voided=%d keys=%d applies=%d replays=%d key_reused=%d rejections=%d", rep.Settled, rep.Voided,
		rep.Keys, rep.Applies, rep.Replays, rep.KeyReused, rep.Rejections)
	fmt.Fprintf(&b, " reads=%d reads_ok=%d reads_failed=%d", rep.Reads, rep.ReadsCompleted, rep.ReadsFailed)
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

// printSummary writes the per-scenario table and the invariant table.
func printSummary(w io.Writer, sums []*summary) {
	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "scenario\truns\tsteps\telections\tcrashes\tpartitions\tsettled\tkeys\treplays\tkey_reused\tresult")
	total := make(map[string]int)
	for _, s := range sums {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n", scenarioLabel(s.scenario), s.runs, s.steps,
			s.elections, s.crashes, s.partitions, s.settled, s.keys, s.replays, s.keyReused, "ok")
		for k, v := range s.checks {
			total[k] += v
		}
	}
	tw.Flush()
	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "invariant\tchecks\tresult\tdescription")
	for _, inv := range sim.Invariants {
		result := "ok"
		if total[inv.ID] == 0 {
			result = "not evaluated"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", inv.ID, total[inv.ID], result, inv.Desc)
	}
	tw.Flush()
}

func replayCommand(p sim.Params) string {
	return fmt.Sprintf("go run ./cmd/chaos -seed %d -nodes %d -steps %d -liveness-steps %d -drop %g -dup %g -scenario %q",
		p.Seed, p.Nodes, p.Steps, p.LivenessSteps, p.Faults.DropP, p.Faults.DupP, p.Scenario)
}
