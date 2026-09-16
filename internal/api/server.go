// Package api serves the HTTP JSON API of one replica with net/http only:
// commands with idempotency keys, forwarding to the leader, consistent and
// stale reads, and RFC 9457 problem details for every error.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// Backend is what the API needs from a replica; replica.Runner satisfies it.
type Backend interface {
	// Submit proposes cmd and waits for its result.
	Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error)
	// Read runs fn against the state, after a read-index barrier when
	// consistent is set.
	Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error
	// Status returns the replica's snapshot.
	Status() replica.Status
}

// Header names of the API.
const (
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderForwarded      = "X-Arena-Forwarded"
	HeaderAppliedSlot    = "X-Arena-Applied-Slot"
	HeaderNode           = "X-Arena-Node"
)

// Config configures a Server.
type Config struct {
	// Self is this replica.
	Self paxos.NodeID
	// Peers maps every node to its base URL, for forwarding.
	Peers map[paxos.NodeID]string
	// RequestTimeout bounds the wait for a command to be applied and for a
	// consistent read. Default 5s.
	RequestTimeout time.Duration
	// MaxBody bounds a request body. Default 1<<20.
	MaxBody int64
	// Seed generates the deal seed of a new tournament. Default crypto/rand.
	Seed func() uint64
	// Now stamps a command's receipt time. Default time.Now.
	Now func() time.Time
	// Client forwards requests to the leader. Default: a client whose
	// timeout is RequestTimeout plus one second.
	Client *http.Client
}

func (c Config) withDefaults() Config {
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.MaxBody == 0 {
		c.MaxBody = 1 << 20
	}
	if c.Seed == nil {
		c.Seed = randomSeed
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Client == nil {
		c.Client = &http.Client{Timeout: c.RequestTimeout + time.Second}
	}
	return c
}

func randomSeed() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("api: crypto/rand: " + err.Error())
	}
	return binary.LittleEndian.Uint64(b[:])
}

// Server holds the handlers. The in-flight map is an optimisation that
// answers a concurrent retry of a key still being processed with 409; the
// state machine, not this map, decides what a repeated key returns.
type Server struct {
	cfg     Config
	backend Backend
	log     *slog.Logger

	mu       sync.Mutex
	inflight map[tournament.IdempotencyKey][32]byte
}

// New builds a server. A nil log discards records.
func New(cfg Config, b Backend, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{cfg: cfg.withDefaults(), backend: b, log: log.With("node", cfg.Self),
		inflight: make(map[tournament.IdempotencyKey][32]byte)}
}

// Handler returns the routed handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tournaments", s.handleCreate)
	mux.HandleFunc("POST /v1/tournaments/{id}/entries", s.handleJoin)
	mux.HandleFunc("POST /v1/tournaments/{id}/scores", s.handleScore)
	mux.HandleFunc("POST /v1/tournaments/{id}/close", s.handleClose)
	mux.HandleFunc("POST /v1/tournaments/{id}/settle", s.handleSettle)
	mux.HandleFunc("GET /v1/tournaments/{id}", s.handleGet)
	mux.HandleFunc("GET /v1/tournaments/{id}/ledger", s.handleLedger)
	mux.HandleFunc("GET /v1/node", s.handleNode)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return s.stamp(mux)
}

// stamp adds the node header to every response.
func (s *Server) stamp(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderNode, strconv.FormatUint(uint64(s.cfg.Self), 10))
		next.ServeHTTP(w, r)
	})
}

// --- responses ---

// Problem is the RFC 9457 error body. Code is the machine-readable reason:
// a tournament.Code for state-machine rejections, or one of the API codes.
type Problem struct {
	Type     string       `json:"type"`
	Title    string       `json:"title"`
	Status   int          `json:"status"`
	Detail   string       `json:"detail,omitempty"`
	Code     string       `json:"code"`
	Replayed bool         `json:"replayed,omitempty"`
	Leader   paxos.NodeID `json:"leader,omitempty"`
}

// API error codes, beyond the tournament codes.
const (
	CodeMissingKey     = "missing_idempotency_key"
	CodeInvalidKey     = "invalid_idempotency_key"
	CodeMalformedBody  = "malformed_body"
	CodeBodyTooLarge   = "body_too_large"
	CodeInFlight       = "in_flight"
	CodeNotLeader      = "not_leader"
	CodeNotReady       = "not_ready"
	CodeForwardFailed  = "forward_failed"
	CodeOutcomeUnknown = "outcome_unknown"
	CodeUnavailable    = "unavailable"
	CodeNotFound       = "not_found"
)

func writeProblem(w http.ResponseWriter, p Problem) {
	if p.Type == "" {
		p.Type = "about:blank"
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// CommandResponse is the success body of every POST.
type CommandResponse struct {
	Code       tournament.Code        `json:"code"`
	Replayed   bool                   `json:"replayed"`
	Slot       paxos.Slot             `json:"slot"`
	Seed       uint64                 `json:"seed,omitempty"`
	Tournament *tournament.Tournament `json:"tournament,omitempty"`
}

// TournamentResponse is the body of GET /v1/tournaments/{id}.
type TournamentResponse struct {
	Tournament  tournament.Tournament `json:"tournament"`
	AppliedSlot paxos.Slot            `json:"applied_slot"`
	Consistent  bool                  `json:"consistent"`
}

// LedgerResponse is the body of GET /v1/tournaments/{id}/ledger.
type LedgerResponse struct {
	Tournament  tournament.TournamentID `json:"tournament"`
	Postings    []ledger.Posting        `json:"postings"`
	AppliedSlot paxos.Slot              `json:"applied_slot"`
	Consistent  bool                    `json:"consistent"`
}

// NodeResponse is the body of GET /v1/node.
type NodeResponse struct {
	replica.Status
	Peers map[paxos.NodeID]string `json:"peers"`
}

// --- request bodies ---

type createRequest struct {
	ID    tournament.TournamentID `json:"id"`
	Rules tournament.Rules        `json:"rules"`
}

type joinRequest struct {
	Player tournament.Player `json:"player"`
}

type scoreRequest struct {
	Player      tournament.PlayerID `json:"player"`
	Score       int64               `json:"score"`
	DealSeed    uint64              `json:"deal_seed"`
	InputDigest tournament.Digest   `json:"input_digest"`
}

type closeRequest struct{}

type settleRequest struct {
	Exclusions tournament.Exclusions `json:"exclusions"`
}

// --- command handlers ---

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	s.command(w, r, http.StatusCreated, func(_ string, body []byte) (tournament.Op, error) {
		var req createRequest
		if err := decodeStrict(body, &req); err != nil {
			return nil, err
		}
		return tournament.CreateTournament{ID: req.ID, Seed: s.cfg.Seed(), Rules: req.Rules}, nil
	})
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	s.command(w, r, http.StatusCreated, func(id string, body []byte) (tournament.Op, error) {
		var req joinRequest
		if err := decodeStrict(body, &req); err != nil {
			return nil, err
		}
		return tournament.Join{Tournament: tournament.TournamentID(id), Player: req.Player}, nil
	})
}

func (s *Server) handleScore(w http.ResponseWriter, r *http.Request) {
	s.command(w, r, http.StatusOK, func(id string, body []byte) (tournament.Op, error) {
		var req scoreRequest
		if err := decodeStrict(body, &req); err != nil {
			return nil, err
		}
		return tournament.SubmitScore{Tournament: tournament.TournamentID(id), Player: req.Player,
			Score: req.Score, DealSeed: req.DealSeed, InputDigest: req.InputDigest}, nil
	})
}

func (s *Server) handleClose(w http.ResponseWriter, r *http.Request) {
	s.command(w, r, http.StatusOK, func(id string, body []byte) (tournament.Op, error) {
		var req closeRequest
		if len(bytes.TrimSpace(body)) > 0 {
			if err := decodeStrict(body, &req); err != nil {
				return nil, err
			}
		}
		return tournament.Close{Tournament: tournament.TournamentID(id)}, nil
	})
}

func (s *Server) handleSettle(w http.ResponseWriter, r *http.Request) {
	s.command(w, r, http.StatusOK, func(id string, body []byte) (tournament.Op, error) {
		var req settleRequest
		if err := decodeStrict(body, &req); err != nil {
			return nil, err
		}
		return tournament.Settle{Tournament: tournament.TournamentID(id), Exclusions: req.Exclusions}, nil
	})
}

// decodeStrict parses exactly one JSON object, rejecting unknown fields
// and trailing data.
func decodeStrict(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("trailing data after the JSON body")
	}
	return nil
}

// command is the shared flow of every POST: key, body, fingerprint,
// in-flight check, submit, and the mapping of results and errors to
// responses.
func (s *Server) command(w http.ResponseWriter, r *http.Request, successStatus int, build func(id string, body []byte) (tournament.Op, error)) {
	key := tournament.IdempotencyKey(strings.TrimSpace(r.Header.Get(HeaderIdempotencyKey)))
	if key == "" {
		writeProblem(w, Problem{Status: http.StatusBadRequest, Code: CodeMissingKey,
			Detail: "every POST requires an " + HeaderIdempotencyKey + " header"})
		return
	}
	if err := tournament.ValidateKey(key); err != nil {
		writeProblem(w, Problem{Status: http.StatusBadRequest, Code: CodeInvalidKey, Detail: err.Error()})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeProblem(w, Problem{Status: http.StatusRequestEntityTooLarge, Code: CodeBodyTooLarge,
				Detail: fmt.Sprintf("body exceeds %d bytes", s.cfg.MaxBody)})
			return
		}
		writeProblem(w, Problem{Status: http.StatusBadRequest, Code: CodeMalformedBody, Detail: err.Error()})
		return
	}
	op, err := build(r.PathValue("id"), body)
	if err != nil {
		writeProblem(w, Problem{Status: http.StatusBadRequest, Code: CodeMalformedBody, Detail: err.Error()})
		return
	}
	cmd := tournament.Command{Key: key, ReceivedAt: s.cfg.Now().UnixMilli(), Op: op}
	fp := tournament.Fingerprint(cmd)

	s.mu.Lock()
	if _, busy := s.inflight[key]; busy {
		s.mu.Unlock()
		writeProblem(w, Problem{Status: http.StatusConflict, Code: CodeInFlight,
			Detail: "a request with this idempotency key is being processed"})
		return
	}
	s.inflight[key] = fp
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inflight, key)
		s.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	res, err := s.backend.Submit(ctx, cmd)
	if err != nil {
		s.submitError(w, r, body, err)
		return
	}
	switch res.Code {
	case tournament.OK:
		resp := CommandResponse{Code: res.Code, Replayed: res.Replayed, Slot: res.Slot, Seed: res.Seed}
		tid := tournament.TournamentOf(op)
		s.backend.Read(ctx, false, func(st *tournament.State) error {
			if t, ok := st.Tournament(tid); ok {
				resp.Tournament = &t
			}
			return nil
		})
		writeJSON(w, successStatus, resp)
	case tournament.KeyReused:
		writeProblem(w, Problem{Status: http.StatusUnprocessableEntity, Code: string(res.Code), Detail: res.Detail})
	default:
		writeProblem(w, Problem{Status: http.StatusConflict, Code: string(res.Code), Detail: res.Detail, Replayed: res.Replayed})
	}
}

// submitError maps a Submit error to a forward or a problem response.
func (s *Server) submitError(w http.ResponseWriter, r *http.Request, body []byte, err error) {
	var nl replog.ErrNotLeader
	switch {
	case errors.As(err, &nl):
		s.forwardOrRefuse(w, r, body, nl.Leader)
	case errors.Is(err, replica.ErrLeadershipLost):
		s.forwardOrRefuse(w, r, body, 0)
	case errors.Is(err, context.DeadlineExceeded):
		writeProblem(w, Problem{Status: http.StatusGatewayTimeout, Code: CodeOutcomeUnknown,
			Detail: "the command was not applied within the request timeout; retry with the same idempotency key"})
	case errors.Is(err, context.Canceled):
		// The client went away; nothing useful can be written.
		writeProblem(w, Problem{Status: http.StatusGatewayTimeout, Code: CodeOutcomeUnknown, Detail: "request cancelled"})
	default:
		w.Header().Set("Retry-After", "1")
		writeProblem(w, Problem{Status: http.StatusServiceUnavailable, Code: CodeUnavailable, Detail: err.Error()})
	}
}

// forwardOrRefuse forwards the request once to the leader it knows, or
// answers 503 with Retry-After when the request was already forwarded or
// no leader is known.
func (s *Server) forwardOrRefuse(w http.ResponseWriter, r *http.Request, body []byte, leader paxos.NodeID) {
	if leader == 0 {
		leader = s.backend.Status().Leader
	}
	if leader == s.cfg.Self {
		// Leadership changed under the request; the client's retry lands on
		// the fresh leader state.
		leader = 0
	}
	url, known := s.cfg.Peers[leader]
	if r.Header.Get(HeaderForwarded) != "" || !known {
		w.Header().Set("Retry-After", "1")
		detail := "this replica is not the leader and no leader is known"
		if known {
			detail = fmt.Sprintf("this replica is not the leader; node %d leads", leader)
		}
		writeProblem(w, Problem{Status: http.StatusServiceUnavailable, Code: CodeNotLeader, Detail: detail, Leader: leader})
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		writeProblem(w, Problem{Status: http.StatusServiceUnavailable, Code: CodeForwardFailed, Detail: err.Error()})
		return
	}
	for _, h := range []string{HeaderIdempotencyKey, "Content-Type", "Accept"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set(HeaderForwarded, "1")
	resp, err := s.cfg.Client.Do(req)
	if err != nil {
		w.Header().Set("Retry-After", "1")
		writeProblem(w, Problem{Status: http.StatusServiceUnavailable, Code: CodeForwardFailed,
			Detail: fmt.Sprintf("forward to node %d failed: %v", leader, err), Leader: leader})
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Retry-After", HeaderAppliedSlot, HeaderNode} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, io.LimitReader(resp.Body, s.cfg.MaxBody))
}

// --- reads ---

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := tournament.TournamentID(r.PathValue("id"))
	consistent := r.URL.Query().Get("read") != "stale"
	var resp TournamentResponse
	found := false
	s.read(w, r, consistent, func(st *tournament.State) error {
		resp.Tournament, found = st.Tournament(id)
		resp.AppliedSlot = st.Applied()
		return nil
	}, func() {
		resp.Consistent = consistent
		w.Header().Set(HeaderAppliedSlot, strconv.FormatUint(uint64(resp.AppliedSlot), 10))
		if !found {
			writeProblem(w, Problem{Status: http.StatusNotFound, Code: CodeNotFound, Detail: fmt.Sprintf("no tournament %q", id)})
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func (s *Server) handleLedger(w http.ResponseWriter, r *http.Request) {
	id := tournament.TournamentID(r.PathValue("id"))
	consistent := r.URL.Query().Get("read") != "stale"
	resp := LedgerResponse{Tournament: id, Postings: []ledger.Posting{}}
	found := false
	s.read(w, r, consistent, func(st *tournament.State) error {
		_, found = st.Tournament(id)
		if ps := st.Ledger().ForTournament(string(id)); ps != nil {
			resp.Postings = ps
		}
		resp.AppliedSlot = st.Applied()
		return nil
	}, func() {
		resp.Consistent = consistent
		w.Header().Set(HeaderAppliedSlot, strconv.FormatUint(uint64(resp.AppliedSlot), 10))
		if !found {
			writeProblem(w, Problem{Status: http.StatusNotFound, Code: CodeNotFound, Detail: fmt.Sprintf("no tournament %q", id)})
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// read runs fn through the backend and then done on success; a not-leader
// error on a consistent read forwards once or answers 503.
func (s *Server) read(w http.ResponseWriter, r *http.Request, consistent bool, fn func(*tournament.State) error, done func()) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	err := s.backend.Read(ctx, consistent, fn)
	if err == nil {
		done()
		return
	}
	var nl replog.ErrNotLeader
	switch {
	case errors.As(err, &nl):
		s.forwardOrRefuse(w, r, nil, nl.Leader)
	case errors.Is(err, replog.ErrNotReady):
		w.Header().Set("Retry-After", "1")
		writeProblem(w, Problem{Status: http.StatusServiceUnavailable, Code: CodeNotReady, Detail: err.Error()})
	case errors.Is(err, context.DeadlineExceeded):
		writeProblem(w, Problem{Status: http.StatusGatewayTimeout, Code: CodeOutcomeUnknown, Detail: "the read barrier did not complete within the request timeout"})
	default:
		w.Header().Set("Retry-After", "1")
		writeProblem(w, Problem{Status: http.StatusServiceUnavailable, Code: CodeUnavailable, Detail: err.Error()})
	}
}

func (s *Server) handleNode(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, NodeResponse{Status: s.backend.Status(), Peers: s.cfg.Peers})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "ok\n")
}
