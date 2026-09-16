package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/intent"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
)

// playFlags are the play API's command-line flags
// (docs/UNITY-INTEGRATION.md section 10.4).
type playFlags struct {
	listen   string
	urls     string
	proxies  string
	keysFile string
	ttl      time.Duration
	dealFile string
}

// playSetup is the validated play configuration shared by every replica of
// the process.
type playSetup struct {
	listen  string
	urls    map[paxos.NodeID]string // from -play-urls; nil when derived
	proxies []netip.Addr
	keys    session.Keyring
	ttl     time.Duration
	deal    []byte
}

// playWriteTimeout bounds a play response; an events request waits up to
// intent.MaxEventsWait before it answers.
const playWriteTimeout = intent.MaxEventsWait + 20*time.Second

// loadPlay validates the play flags and loads the keyring and the deal
// secret from their files or environment variables. It returns nil when
// -play-listen is empty. No error message contains key material.
func loadPlay(f playFlags, getenv func(string) string) (*playSetup, error) {
	if f.listen == "" {
		if f.urls != "" || f.proxies != "" || f.keysFile != "" || f.dealFile != "" {
			return nil, errors.New("-play-urls, -play-trusted-proxies, -session-keys-file and -deal-secret-file require -play-listen")
		}
		return nil, nil
	}
	p := &playSetup{listen: f.listen, ttl: f.ttl}
	if f.ttl <= 0 || f.ttl > session.MaxTTL {
		return nil, fmt.Errorf("-session-ttl %v must be above 0 and at most %v", f.ttl, session.MaxTTL)
	}
	spec, source := getenv(session.EnvKeys), session.EnvKeys
	if f.keysFile != "" {
		b, err := os.ReadFile(f.keysFile)
		if err != nil {
			return nil, fmt.Errorf("-session-keys-file: %w", err)
		}
		spec, source = string(b), "-session-keys-file"
	}
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("-play-listen needs session keys: set -session-keys-file or %s (id=hex, at least 32 bytes each)", session.EnvKeys)
	}
	keys, err := session.ParseKeyring(spec)
	if err != nil {
		return nil, fmt.Errorf("session keys from %s: %w", source, err)
	}
	p.keys = keys
	secret, source := getenv(intent.EnvDealSecret), intent.EnvDealSecret
	if f.dealFile != "" {
		b, err := os.ReadFile(f.dealFile)
		if err != nil {
			return nil, fmt.Errorf("-deal-secret-file: %w", err)
		}
		secret, source = string(b), "-deal-secret-file"
	}
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return nil, fmt.Errorf("-play-listen needs a deal secret: set -deal-secret-file or %s (hex, at least %d bytes)", intent.EnvDealSecret, intent.MinDealSecretBytes)
	}
	deal, err := hex.DecodeString(secret)
	if err != nil || len(deal) < intent.MinDealSecretBytes {
		return nil, fmt.Errorf("the deal secret from %s must be hex of at least %d bytes", source, intent.MinDealSecretBytes)
	}
	p.deal = deal
	if f.urls != "" {
		urls, _, err := parsePeers(f.urls)
		if err != nil {
			return nil, fmt.Errorf("-play-urls: %s", strings.TrimPrefix(err.Error(), "-peers: "))
		}
		p.urls = urls
	}
	for _, part := range strings.Split(f.proxies, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		a, err := netip.ParseAddr(part)
		if err != nil {
			return nil, fmt.Errorf("-play-trusted-proxies: %q is not an IP address", part)
		}
		p.proxies = append(p.proxies, a)
	}
	return p, nil
}

// server builds the play API of replica self.
func (p *playSetup) server(self paxos.NodeID, urls map[paxos.NodeID]string, b intent.Backend, logger *slog.Logger) (http.Handler, error) {
	s, err := intent.New(intent.Config{
		Self: self, PublicURLs: urls, Keys: p.keys, SessionTTL: p.ttl, DealSecret: p.deal, TrustedProxies: p.proxies,
	}, b, logger)
	if err != nil {
		return nil, err
	}
	return s.Handler(), nil
}

// newPlayHTTPServer is newHTTPServer with a write timeout that outlasts an
// events long-poll.
func newPlayHTTPServer(h http.Handler, logger *slog.Logger) *http.Server {
	srv := newHTTPServer(h, logger)
	srv.WriteTimeout = playWriteTimeout
	return srv
}

// playListeners opens the play listeners of ids on consecutive ports from
// the -play-listen address, and returns them with each replica's public
// URL: from -play-urls when given, otherwise http:// and the address.
func (p *playSetup) playListeners(ids []paxos.NodeID) (map[paxos.NodeID]net.Listener, map[paxos.NodeID]string, error) {
	host, portStr, err := net.SplitHostPort(p.listen)
	if err != nil {
		return nil, nil, fmt.Errorf("-play-listen: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, nil, fmt.Errorf("-play-listen: bad port %q", portStr)
	}
	lns := make(map[paxos.NodeID]net.Listener, len(ids))
	urls := make(map[paxos.NodeID]string, len(ids))
	for i, id := range ids {
		addr := net.JoinHostPort(host, "0")
		if port != 0 {
			addr = net.JoinHostPort(host, strconv.Itoa(port+i))
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, l := range lns {
				l.Close()
			}
			return nil, nil, fmt.Errorf("node %d: play listen %s: %w", id, addr, err)
		}
		lns[id] = ln
		urls[id] = "http://" + ln.Addr().String()
		if u, ok := p.urls[id]; ok {
			urls[id] = u
		}
	}
	return lns, urls, nil
}

// printPlayInstructions prints how to reach the play API.
func printPlayInstructions(w io.Writer, ids []paxos.NodeID, urls map[paxos.NodeID]string, operator string) {
	fmt.Fprintln(w, "Play API for game clients (docs/UNITY-INTEGRATION.md): followers answer POSTs with 307 to the leader.")
	for _, id := range ids {
		fmt.Fprintf(w, "  PLAY node %d  %s\n", id, urls[id])
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  OP=%s\n", operator)
	fmt.Fprintf(w, "  PLAY=%s\n", urls[ids[0]])
	fmt.Fprintln(w, "  curl -s -X POST $OP/v1/tournaments -H 'Idempotency-Key: create-ladder' -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `    -d '{"id":"ladder-1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[10000],"min_entrants":1,"max_entrants":100,"max_score":14400,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":1,"jurisdictions":[]},"game":"ladder-v1"}}'`)
	fmt.Fprintln(w, "  DEV=$(openssl rand -hex 16); SECRET=$(openssl rand -hex 32)")
	fmt.Fprintln(w, "  curl -s -L -X POST $PLAY/v1/session -H \"Idempotency-Key: session-$DEV\" -H 'Content-Type: application/json' \\")
	fmt.Fprintln(w, `    -d "{\"device_id\":\"$DEV\",\"device_secret\":\"$SECRET\",\"jurisdiction\":\"TR\",\"age\":30}"`)
	fmt.Fprintln(w, "  # then, with TOKEN from session_token: POST $PLAY/v1/tournaments/ladder-1/join with {\"seq\":1},")
	fmt.Fprintln(w, "  # .../rounds/1/deal with {\"seq\":2}, .../rounds/1/moves with {\"seq\":3,\"move_index\":0,\"kind\":\"draw\",\"column\":-1}")
	fmt.Fprintln(w)
}
