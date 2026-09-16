package intent

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/jsonx"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// DefaultRequestTimeout is the default of Config.RequestTimeout.
const DefaultRequestTimeout = 5 * time.Second

// postCommitReadTimeout bounds the read that builds a command's response
// after the command was applied.
const postCommitReadTimeout = 2 * time.Second

// NoLimits disables every rate limit: a Rate whose Burst is negative does
// not limit. Tests use it; a deployment keeps DefaultLimits.
var NoLimits = Limits{
	SessionPerDevice: Rate{Burst: -1}, SessionPerAddr: Rate{Burst: -1},
	Intents: Rate{Burst: -1}, Reads: Rate{Burst: -1}, EventPolls: Rate{Burst: -1},
}

// New builds a Server. It refuses a keyring that session.Keyring.Validate
// refuses, a deal secret shorter than MinDealSecretBytes, a token lifetime
// above session.MaxTTL, and public URLs that do not name this replica or
// are not http(s) base URLs. A nil log discards records.
func New(cfg Config, b Backend, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if err := cfg.Keys.Validate(); err != nil {
		return nil, fmt.Errorf("intent: session keys: %w", err)
	}
	if len(cfg.DealSecret) < MinDealSecretBytes {
		return nil, fmt.Errorf("intent: the deal secret has %d bytes, at least %d are required", len(cfg.DealSecret), MinDealSecretBytes)
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = session.DefaultTTL
	}
	if cfg.SessionTTL < time.Millisecond || cfg.SessionTTL > session.MaxTTL {
		return nil, fmt.Errorf("intent: the session lifetime %v is outside 1ms..%v", cfg.SessionTTL, session.MaxTTL)
	}
	urls := make(map[paxos.NodeID]string, len(cfg.PublicURLs))
	for id, u := range cfg.PublicURLs {
		u = strings.TrimRight(u, "/")
		if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
			return nil, fmt.Errorf("intent: public URL %q of node %d is not an http(s) base URL", u, id)
		}
		urls[id] = u
	}
	if _, ok := urls[cfg.Self]; !ok {
		return nil, fmt.Errorf("intent: no public URL for this node %d", cfg.Self)
	}
	cfg.PublicURLs = urls
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = DefaultMaxBody
	}
	if cfg.MinSlotWait <= 0 {
		cfg.MinSlotWait = DefaultMinSlotWait
	}
	if cfg.MaxEventPolls <= 0 {
		cfg.MaxEventPolls = DefaultMaxEventPolls
	}
	if cfg.Limits == (Limits{}) {
		cfg.Limits = DefaultLimits
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewPlayerID == nil {
		cfg.NewPlayerID = RandomPlayerID
	}
	proxies := make(map[netip.Addr]bool, len(cfg.TrustedProxies))
	for _, p := range cfg.TrustedProxies {
		proxies[p.Unmap()] = true
	}
	s := &Server{
		cfg:      cfg,
		backend:  b,
		log:      log.With("node", cfg.Self),
		proxies:  proxies,
		inflight: make(map[tournament.IdempotencyKey]struct{}),
		polls:    make(map[tournament.PlayerID]*poll),
	}
	s.sessionDevice = newLimiter(cfg.Limits.SessionPerDevice)
	s.sessionAddr = newLimiter(cfg.Limits.SessionPerAddr)
	s.intentLimit = newLimiter(cfg.Limits.Intents)
	s.readLimit = newLimiter(cfg.Limits.Reads)
	s.pollLimit = newLimiter(cfg.Limits.EventPolls)
	return s, nil
}

var base32Lower = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// RandomPlayerID draws a player id: PlayerIDPrefix and PlayerIDRandomLen
// lower-case base32 characters encoding 130 bits from crypto/rand.
func RandomPlayerID() tournament.PlayerID {
	var b [17]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("intent: crypto/rand: " + err.Error())
	}
	return tournament.PlayerID(PlayerIDPrefix + base32Lower.EncodeToString(b[:])[:PlayerIDRandomLen])
}

// Handler returns the routed handler of the play API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range []struct {
		pattern string
		h       http.HandlerFunc
	}{
		{RouteSession, s.handleSession},
		{RouteTournaments, s.handleTournaments},
		{RouteJoin, s.handleJoin},
		{RouteDeal, s.handleDeal},
		{RouteRound, s.handleRound},
		{RouteMove, s.handleMove},
		{RouteFinish, s.handleFinish},
		{RouteLeaderboard, s.handleLeaderboard},
		{RouteClaim, s.handleClaim},
		{RouteEvents, s.handleEvents},
	} {
		method, path, _ := strings.Cut(rt.pattern, " ")
		mux.HandleFunc(path, s.method(method, rt.h))
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, http.StatusNotFound, CodeNotFound, "no route "+r.URL.Path, false)
	})
	return s.stamp(mux)
}

// method answers 405 unless the request uses m. Patterns are registered
// without methods so that a known path with another method is a 405 with a
// JSON body rather than the mux's plain-text answer.
func (s *Server) method(m string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != m {
			w.Header().Set("Allow", m)
			s.writeError(w, http.StatusMethodNotAllowed, CodeMethodNotAllowed, r.Method+" is not allowed on this route", false)
			return
		}
		h(w, r)
	}
}

// stamp sets the headers every response carries before any handler runs.
func (s *Server) stamp(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := s.backend.Status()
		w.Header().Set(HeaderNode, strconv.FormatUint(uint64(s.cfg.Self), 10))
		w.Header().Set(HeaderLeader, s.cfg.PublicURLs[st.Leader])
		next.ServeHTTP(w, r)
	})
}

// --- writing ---

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.log.Error("encode response", "err", err)
		status = http.StatusInternalServerError
		body, _ = json.Marshal(ErrorBody{Code: CodeInternal, Message: "response encoding failed", Retryable: true})
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set(HeaderServerTime, strconv.FormatInt(s.cfg.Now().UnixMilli(), 10))
	w.WriteHeader(status)
	w.Write(body)
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, message string, retryable bool) {
	s.writeJSON(w, status, ErrorBody{Code: code, Message: message, Retryable: retryable})
}

func retryAfter(w http.ResponseWriter, d time.Duration) {
	secs := int64((d + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	w.Header().Set(HeaderRetryAfter, strconv.FormatInt(secs, 10))
}

// --- leader handling ---

// redirect answers 307 towards leader when this replica knows its public
// URL, and 503 no_leader otherwise.
func (s *Server) redirect(w http.ResponseWriter, r *http.Request, leader paxos.NodeID) {
	if leader != 0 && leader != s.cfg.Self {
		if u, ok := s.cfg.PublicURLs[leader]; ok {
			w.Header().Set(HeaderLocation, u+r.URL.RequestURI())
			w.Header().Set(HeaderLeader, u)
			s.writeError(w, http.StatusTemporaryRedirect, CodeNotLeader,
				fmt.Sprintf("node %d leads; send this request there", leader), true)
			return
		}
	}
	retryAfter(w, time.Second)
	s.writeError(w, http.StatusServiceUnavailable, CodeNoLeader, "no leader is known; retry the same request", true)
}

// onLeader answers the request itself and returns false unless this replica
// is a ready leader.
func (s *Server) onLeader(w http.ResponseWriter, r *http.Request) bool {
	st := s.backend.Status()
	if st.Role != replog.Leader {
		s.redirect(w, r, st.Leader)
		return false
	}
	if !st.Ready {
		retryAfter(w, time.Second)
		s.writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the leader has not committed its leadership yet; retry the same request", true)
		return false
	}
	return true
}

// --- request checks ---

// validClientKey reports whether k is MinClientKeyLen to MaxClientKeyLen
// characters from A-Z, a-z, 0-9, '_' and '-'.
func validClientKey(k string) bool {
	if len(k) < MinClientKeyLen || len(k) > MaxClientKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func (s *Server) clientKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	k := r.Header.Get(HeaderIdempotencyKey)
	if k == "" {
		s.writeError(w, http.StatusBadRequest, CodeMissingKey, "every POST requires an "+HeaderIdempotencyKey+" header", false)
		return "", false
	}
	if !validClientKey(k) {
		s.writeError(w, http.StatusBadRequest, CodeInvalidKey,
			fmt.Sprintf("an %s is %d to %d characters from A-Z, a-z, 0-9, '_' and '-'", HeaderIdempotencyKey, MinClientKeyLen, MaxClientKeyLen), false)
		return "", false
	}
	return k, true
}

// authenticate verifies the bearer token.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (session.Claims, bool) {
	h := r.Header.Get(HeaderAuthorization)
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || tok == "" {
		s.writeError(w, http.StatusUnauthorized, CodeSessionMissing, "an Authorization: Bearer <session_token> header is required", true)
		return session.Claims{}, false
	}
	c, err := s.cfg.Keys.Verify(tok, s.cfg.Now().UnixMilli())
	switch {
	case errors.Is(err, session.ErrExpired):
		s.writeError(w, http.StatusUnauthorized, CodeSessionExpired, "the session has expired; open a new session", true)
		return session.Claims{}, false
	case err != nil:
		s.writeError(w, http.StatusUnauthorized, CodeSessionInvalid, "the session token is not valid; open a new session", true)
		return session.Claims{}, false
	}
	return c, true
}

func (s *Server) allow(w http.ResponseWriter, l *limiter, key string) bool {
	wait, ok := l.allow(key, s.cfg.Now())
	if ok {
		return true
	}
	retryAfter(w, wait)
	s.writeError(w, http.StatusTooManyRequests, CodeRateLimited, "too many requests; retry the same request after Retry-After", true)
	return false
}

// readBody reads a body of at most MaxBody bytes and decodes it strictly
// into v.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			s.writeError(w, http.StatusRequestEntityTooLarge, CodeBodyTooLarge, fmt.Sprintf("the body exceeds %d bytes", s.cfg.MaxBody), false)
			return false
		}
		s.writeError(w, http.StatusBadRequest, CodeMalformed, "the body could not be read", false)
		return false
	}
	if err := jsonx.DecodeStrict(body, v); err != nil {
		s.writeError(w, http.StatusBadRequest, CodeMalformed, "the body is not the documented JSON object: "+err.Error(), false)
		return false
	}
	return true
}

func (s *Server) malformed(w http.ResponseWriter, msg string) {
	s.writeError(w, http.StatusBadRequest, CodeMalformed, msg, false)
}

// clientAddr is the TCP peer's address, or the first X-Forwarded-For
// address when the peer is a trusted proxy.
func (s *Server) clientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if a, err := netip.ParseAddr(host); err == nil && s.proxies[a.Unmap()] {
		first, _, _ := strings.Cut(r.Header.Get("X-Forwarded-For"), ",")
		if fa, err := netip.ParseAddr(strings.TrimSpace(first)); err == nil {
			return fa.Unmap().String()
		}
	}
	return host
}

// lock marks a namespaced key in flight on this replica.
func (s *Server) lock(w http.ResponseWriter, key tournament.IdempotencyKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.inflight[key]; busy {
		retryAfter(w, time.Second)
		s.writeError(w, http.StatusConflict, CodeInFlight, "a request with this idempotency key is being processed; retry the same request", true)
		return false
	}
	s.inflight[key] = struct{}{}
	return true
}

func (s *Server) unlock(key tournament.IdempotencyKey) {
	s.mu.Lock()
	delete(s.inflight, key)
	s.mu.Unlock()
}

// submit proposes cmd and waits for its result; on failure it has answered
// the request.
func (s *Server) submit(w http.ResponseWriter, r *http.Request, cmd tournament.Command) (tournament.Result, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	res, err := s.backend.Submit(ctx, cmd)
	if err == nil {
		return res, true
	}
	var nl replog.NotLeaderError
	switch {
	case errors.As(err, &nl):
		s.redirect(w, r, nl.Leader)
	case errors.Is(err, replica.ErrLeadershipLost):
		s.redirect(w, r, s.backend.Status().Leader)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		s.writeError(w, http.StatusGatewayTimeout, CodeOutcomeUnknown,
			"the command was not applied within the request timeout; it may still be: retry the same request", true)
	case errors.Is(err, replog.ErrBusy), errors.Is(err, replica.ErrStopped):
		retryAfter(w, time.Second)
		s.writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the leader cannot take the command now; retry the same request", true)
	default:
		s.log.Warn("submit failed", "key", cmd.Key, "err", err)
		s.writeError(w, http.StatusInternalServerError, CodeInternal, "the command could not be submitted; retry the same request", true)
	}
	return tournament.Result{}, false
}

// readState runs fn on this replica's applied state for a response; on
// failure it has answered the request.
func (s *Server) readState(w http.ResponseWriter, fn func(*tournament.State) error) bool {
	ctx, cancel := context.WithTimeout(context.Background(), postCommitReadTimeout)
	defer cancel()
	if err := s.backend.Read(ctx, false, fn); err != nil {
		retryAfter(w, time.Second)
		s.writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the replica could not read its state; retry the same request", true)
		return false
	}
	return true
}

// replica.Runner is the Backend of a running replica.
var _ Backend = (*replica.Runner)(nil)
