// Command arena runs the replicated tournament settlement service.
//
// Without -peers it starts N replicas in one process, connected by an
// in-memory bus, each with its own HTTP port, and prints curl commands that
// exercise them. Their state is in memory and ends with the process.
//
// With -id, -peers and -wal it runs one replica per process over the HTTP
// transport, keeping the replica's promise, accepted values and chosen
// entries in the append-only file named by -wal. A replica killed and
// restarted with the same -wal file rejoins with that state. -wal is
// required in this mode: a replica that restarts without its durable state
// forgets promises and acknowledged commands, and must not rejoin under its
// old identity.
//
// With -play-listen every replica also serves the play API of package intent
// for game clients on a listener of its own (docs/UNITY-INTEGRATION.md). It
// needs a session keyring (-session-keys-file or ARENA_SESSION_KEYS) and a
// deal secret (-deal-secret-file or ARENA_DEAL_SECRET), and in one replica
// per process mode the public play URL of every replica (-play-urls).
//
// With -debug-feed every replica records the Multi-Paxos messages it sends
// and receives, its role changes and its commit progress in a bounded buffer
// and serves them read-only on GET /debug/events of its operator listener
// (package debugfeed), for a visualiser. The feed has no authentication and
// exposes protocol traffic, so the operator listener must then stay on
// loopback.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/api"
	"github.com/oguzhanozfe/paxos-arena/internal/debugfeed"
	"github.com/oguzhanozfe/paxos-arena/internal/intent"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/replog/wal"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "arena:", err)
		}
		os.Exit(1)
	}
}

// shutdownTimeout bounds the graceful stop of an HTTP server.
const shutdownTimeout = 5 * time.Second

// run parses args and runs until ctx is done. It returns nil on a clean
// stop and the first error otherwise.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("arena", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodes := fs.Int("nodes", 3, "replicas to start in this process (ignored with -peers)")
	listen := fs.String("listen", "127.0.0.1:8081", "listen address; with -nodes, node i listens on port+i-1 (port 0 picks free ports)")
	id := fs.Uint("id", 0, "this replica's node id, for one replica per process (requires -peers)")
	peers := fs.String("peers", "", "every replica as id=base-url, comma separated, for one replica per process")
	walPath := fs.String("wal", "", "append-only file holding this replica's durable log state (required with -peers)")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	logJSON := fs.Bool("log-json", false, "log JSON records instead of text")
	debugFeed := fs.Bool("debug-feed", false, "record each replica's protocol messages, roles and commits and serve them on GET "+debugfeed.Path+
		" of the operator listener, without authentication (keep -listen on loopback)")
	var pf playFlags
	fs.StringVar(&pf.listen, "play-listen", "", "address of the play API for game clients; with -nodes, node i uses port+i-1 (empty: play API off)")
	fs.StringVar(&pf.urls, "play-urls", "", "public base URL of every replica's play listener, id=url comma separated (default: http:// and each play address)")
	fs.StringVar(&pf.proxies, "play-trusted-proxies", "", "comma-separated proxy IP addresses whose X-Forwarded-For is believed")
	fs.StringVar(&pf.keysFile, "session-keys-file", "", "file of session keys, one id=hex per line, the signing key first (default: $"+session.EnvKeys+")")
	fs.DurationVar(&pf.ttl, "session-ttl", session.DefaultTTL, "session token lifetime, at most 24h")
	fs.StringVar(&pf.dealFile, "deal-secret-file", "", "file holding the deal secret as hex (default: $"+intent.EnvDealSecret+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	play, err := loadPlay(pf, os.Getenv)
	if err != nil {
		return err
	}
	var level slog.LevelVar
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	opts := &slog.HandlerOptions{Level: &level}
	var handler slog.Handler = slog.NewTextHandler(stderr, opts)
	if *logJSON {
		handler = slog.NewJSONHandler(stderr, opts)
	}
	logger := slog.New(handler)
	if *peers != "" {
		return runSingle(ctx, paxos.NodeID(*id), *peers, *listen, *walPath, play, *debugFeed, logger, stdout)
	}
	if *id != 0 {
		return errors.New("-id requires -peers")
	}
	if *walPath != "" {
		return errors.New("-wal requires -id and -peers: replicas started together with -nodes keep their state in memory")
	}
	return runCluster(ctx, *nodes, *listen, play, *debugFeed, logger, stdout)
}

// newHTTPServer returns a server with every timeout set.
func newHTTPServer(h http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
}

// serve runs srv on ln until ctx is done, then shuts it down gracefully.
func serve(ctx context.Context, srv *http.Server, ln net.Listener) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			srv.Close()
		}
		<-errc
		return nil
	}
}

// seededRNG returns a generator for a node's election timeouts. The seed
// is not secret; it only needs to differ between replicas.
func seededRNG(id paxos.NodeID) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id)))
}

// debugFeedNotice is printed once at startup when -debug-feed is on.
const debugFeedNotice = "arena: -debug-feed is on: GET " + debugfeed.Path + " on every operator listener exposes protocol traffic " +
	"(messages, ballots, slots, roles, command names and ids) without authentication; serve it on loopback only"

// newFeed returns the debug feed of replica id installed on runner, or nil
// when the feed is off.
func newFeed(on bool, id paxos.NodeID, runner *replica.Runner) *debugfeed.Feed {
	if !on {
		return nil
	}
	feed := debugfeed.New(id)
	runner.Observe(feed)
	return feed
}

// runCluster starts n replicas on consecutive ports from listen and prints
// how to exercise them.
func runCluster(ctx context.Context, n int, listen string, play *playSetup, debugFeed bool, logger *slog.Logger, stdout io.Writer) error {
	if n < 1 {
		return errors.New("-nodes must be at least 1")
	}
	host, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("-listen: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("-listen: bad port %q", portStr)
	}
	ids := make([]paxos.NodeID, n)
	listeners := make(map[paxos.NodeID]net.Listener, n)
	urls := make(map[paxos.NodeID]string, n)
	for i := range ids {
		id := paxos.NodeID(i + 1)
		ids[i] = id
		addr := net.JoinHostPort(host, "0")
		if port != 0 {
			addr = net.JoinHostPort(host, strconv.Itoa(port+i))
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, l := range listeners {
				l.Close()
			}
			return fmt.Errorf("node %d: listen %s: %w", id, addr, err)
		}
		listeners[id] = ln
		urls[id] = "http://" + ln.Addr().String()
	}
	var playLns map[paxos.NodeID]net.Listener
	var playURLs map[paxos.NodeID]string
	if play != nil {
		playLns, playURLs, err = play.playListeners(ids)
		if err != nil {
			for _, l := range listeners {
				l.Close()
			}
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bus := transport.NewLocal()
	var wg sync.WaitGroup
	errc := make(chan error, 3*n)
	for _, id := range ids {
		core, err := replica.NewCore(replog.DefaultConfig(id, ids), replog.NewMemStore(), seededRNG(id))
		if err != nil {
			cancel()
			wg.Wait()
			return err
		}
		runner := replica.NewRunner(core, bus.Send, logger)
		feed := newFeed(debugFeed, id, runner)
		bus.Register(id, runner.Deliver)
		var operator http.Handler = api.New(api.Config{Self: id, Peers: urls}, runner, logger).Handler()
		if feed != nil {
			mux := http.NewServeMux()
			mux.Handle(debugfeed.Path, feed.Handler())
			mux.Handle("/", operator)
			operator = mux
		}
		srv := newHTTPServer(operator, logger)
		ln := listeners[id]
		if play != nil {
			h, err := play.server(id, playURLs, runner, logger)
			if err != nil {
				cancel()
				wg.Wait()
				return err
			}
			psrv, pln := newPlayHTTPServer(h, logger), playLns[id]
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := serve(ctx, psrv, pln); err != nil {
					errc <- err
				}
			}()
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := runner.Run(ctx); err != nil {
				errc <- err
			}
		}()
		go func() {
			defer wg.Done()
			if err := serve(ctx, srv, ln); err != nil {
				errc <- err
			}
		}()
	}
	printClusterInstructions(stdout, ids, urls, playURLs)
	if debugFeed {
		fmt.Fprintln(stdout, debugFeedNotice)
	}
	var first error
	select {
	case <-ctx.Done():
	case first = <-errc:
		logger.Error("replica failed", "err", first)
	}
	cancel()
	wg.Wait()
	return first
}

// parsePeers parses "1=http://a:8081,2=http://b:8082".
func parsePeers(spec string) (map[paxos.NodeID]string, []paxos.NodeID, error) {
	out := make(map[paxos.NodeID]string)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, nil, fmt.Errorf("-peers: %q is not id=url", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(k), 10, 32)
		if err != nil || id == 0 {
			return nil, nil, fmt.Errorf("-peers: bad node id %q", k)
		}
		v = strings.TrimRight(strings.TrimSpace(v), "/")
		if !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
			return nil, nil, fmt.Errorf("-peers: %q is not an http(s) base URL", v)
		}
		if _, dup := out[paxos.NodeID(id)]; dup {
			return nil, nil, fmt.Errorf("-peers: node %d listed twice", id)
		}
		out[paxos.NodeID(id)] = v
	}
	if len(out) == 0 {
		return nil, nil, errors.New("-peers: empty")
	}
	ids := make([]paxos.NodeID, 0, len(out))
	for id := range out {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return out, ids, nil
}

// runSingle runs one replica over the HTTP transport with its durable
// state in the file at walPath.
func runSingle(ctx context.Context, self paxos.NodeID, peersSpec, listen, walPath string, play *playSetup, debugFeed bool, logger *slog.Logger, stdout io.Writer) error {
	urls, ids, err := parsePeers(peersSpec)
	if err != nil {
		return err
	}
	if self == 0 {
		return errors.New("-id is required with -peers")
	}
	if _, ok := urls[self]; !ok {
		return fmt.Errorf("-id %d is not in -peers", self)
	}
	if walPath == "" {
		return errors.New("-wal is required with -peers: a replica that restarts without its durable state " +
			"would rejoin under its old id having forgotten its promises and acknowledged commands")
	}
	if play != nil && len(ids) > 1 {
		for _, id := range ids {
			if _, ok := play.urls[id]; !ok {
				return fmt.Errorf("-play-urls must name every replica in -peers (node %d is missing): a follower redirects game clients to the leader's play URL", id)
			}
		}
	}
	store, err := wal.Open(walPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Bind(self, ids); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listen, err)
	}
	core, err := replica.NewCore(replog.DefaultConfig(self, ids), store, seededRNG(self))
	if err != nil {
		ln.Close()
		return err
	}
	var tr *transport.HTTP
	runner := replica.NewRunner(core, func(env replog.Envelope) { tr.Send(env) }, logger)
	feed := newFeed(debugFeed, self, runner)
	tr = transport.NewHTTP(self, urls, runner.Deliver, nil, logger)
	mux := http.NewServeMux()
	mux.Handle("POST "+transport.Path, tr.Handler())
	if feed != nil {
		mux.Handle(debugfeed.Path, feed.Handler())
	}
	mux.Handle("/", api.New(api.Config{Self: self, Peers: urls}, runner, logger).Handler())
	srv := newHTTPServer(mux, logger)

	var psrv *http.Server
	var pln net.Listener
	var playURLs map[paxos.NodeID]string
	if play != nil {
		lns, derived, err := play.playListeners([]paxos.NodeID{self})
		if err != nil {
			ln.Close()
			return err
		}
		pln = lns[self]
		playURLs = derived
		for id, u := range play.urls {
			playURLs[id] = u
		}
		h, err := play.server(self, playURLs, runner, logger)
		if err != nil {
			ln.Close()
			pln.Close()
			return err
		}
		psrv = newPlayHTTPServer(h, logger)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errc := make(chan error, 3)
	if psrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := serve(ctx, psrv, pln); err != nil {
				errc <- err
			}
		}()
	}
	wg.Add(3)
	go func() { defer wg.Done(); tr.Run(ctx) }()
	go func() {
		defer wg.Done()
		if err := serve(ctx, srv, ln); err != nil {
			errc <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := runner.Run(ctx); err != nil {
			errc <- err
		}
	}()
	fmt.Fprintf(stdout, "arena: node %d of %v listening on http://%s, durable state in %s (commit index %d)\n",
		self, ids, ln.Addr(), walPath, core.Log().CommitIndex())
	for _, id := range ids {
		fmt.Fprintf(stdout, "  node %d  %s\n", id, urls[id])
	}
	fmt.Fprintln(stdout)
	printCurl(stdout, urls[self], urls, ids)
	if playURLs != nil {
		fmt.Fprintf(stdout, "arena: node %d play API listening on http://%s\n", self, pln.Addr())
		printPlayInstructions(stdout, ids, playURLs, urls[self])
	}
	if debugFeed {
		fmt.Fprintln(stdout, debugFeedNotice)
	}
	var first error
	select {
	case <-ctx.Done():
	case first = <-errc:
		logger.Error("replica failed", "err", first)
	}
	cancel()
	wg.Wait()
	return first
}

func printClusterInstructions(w io.Writer, ids []paxos.NodeID, urls, playURLs map[paxos.NodeID]string) {
	fmt.Fprintf(w, "arena: %d replicas in one process, connected by an in-memory bus\n", len(ids))
	for _, id := range ids {
		fmt.Fprintf(w, "  node %d  %s\n", id, urls[id])
	}
	fmt.Fprintln(w)
	printCurl(w, urls[ids[0]], urls, ids)
	if playURLs != nil {
		printPlayInstructions(w, ids, playURLs, urls[ids[0]])
	}
	fmt.Fprintln(w, "To run replicas as separate processes, so that one can be killed and restarted, start each with its own log file:")
	fmt.Fprintln(w, "  arena -id <n> -listen <host:port> -wal node<n>.wal -peers 1=http://127.0.0.1:8081,2=http://127.0.0.1:8082,3=http://127.0.0.1:8083")
	fmt.Fprintln(w, "A replica restarted with the same -wal file keeps its promises, accepted values and chosen entries.")
	fmt.Fprintln(w, "One whose file is lost must not rejoin under its old id: start a new cluster instead.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Ctrl-C stops every replica. In this mode state is in memory and is lost on exit.")
}

// printCurl prints the curl commands of one full tournament.
func printCurl(w io.Writer, base string, urls map[paxos.NodeID]string, ids []paxos.NodeID) {
	other := base
	for _, id := range ids {
		if urls[id] != base {
			other = urls[id]
			break
		}
	}
	fmt.Fprintln(w, "Any node accepts commands; a follower forwards to the leader. Every POST needs an Idempotency-Key.")
	fmt.Fprintln(w, "Repeating a POST with the same key returns the recorded result with \"replayed\": true and changes nothing.")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  BASE=%s\n", base)
	fmt.Fprintln(w, "  curl -s $BASE/v1/node")
	fmt.Fprintln(w, "  curl -s -X POST $BASE/v1/tournaments -H 'Idempotency-Key: create-t1' -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `    -d '{"id":"t1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,"max_score":100000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":7,"jurisdictions":["XX"]}}}'`)
	fmt.Fprintln(w, "  for p in p1 p2 p3; do")
	fmt.Fprintln(w, "    curl -s -X POST $BASE/v1/tournaments/t1/entries -H \"Idempotency-Key: join-$p\" -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `      -d "{\"player\":{\"id\":\"$p\",\"jurisdiction\":\"TR\",\"age\":31}}"`)
	fmt.Fprintln(w, "  done")
	fmt.Fprintln(w, "  # the join responses carry \"seed\": the deal every entrant plays; scores must quote it")
	fmt.Fprintln(w, "  SEED=$(curl -s \"$BASE/v1/tournaments/t1\" | sed -n 's/.*\"seed\": *\\([0-9]*\\).*/\\1/p' | head -1)")
	fmt.Fprintln(w, "  i=0; for p in p1 p2 p3; do i=$((i+1))")
	fmt.Fprintln(w, "    curl -s -X POST $BASE/v1/tournaments/t1/scores -H \"Idempotency-Key: score-$p\" -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `      -d "{\"player\":\"$p\",\"score\":$((i*1000)),\"deal_seed\":$SEED}"`)
	fmt.Fprintln(w, "  done")
	fmt.Fprintln(w, "  curl -s -X POST $BASE/v1/tournaments/t1/close -H 'Idempotency-Key: close-t1' -H 'Content-Type: application/json' -d '{}'")
	fmt.Fprintln(w, "  curl -s -X POST $BASE/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1' -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `    -d '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}'`)
	fmt.Fprintln(w, "  curl -s $BASE/v1/tournaments/t1            # consistent read, served by the leader")
	fmt.Fprintln(w, "  curl -s $BASE/v1/tournaments/t1/ledger     # every posting of the tournament")
	fmt.Fprintf(w, "  curl -s -i \"%s/v1/tournaments/t1?read=stale\"   # stale read from another node; see X-Arena-Applied-Slot\n", other)
	fmt.Fprintln(w)
}
