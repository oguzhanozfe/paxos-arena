package intent

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// errNoRecord is a response builder finding no record for a result that
// must have one; the request is answered 500 and the client retries.
var errNoRecord = errors.New("intent: applied state has no record for the result")

// pathTournament parses {tournament_id}.
func (s *Server) pathTournament(w http.ResponseWriter, r *http.Request) (tournament.TournamentID, bool) {
	tid := tournament.TournamentID(r.PathValue("tournament_id"))
	if err := tournament.ValidateTournamentID(tid); err != nil {
		s.malformed(w, "tournament_id: "+err.Error())
		return "", false
	}
	return tid, true
}

// pathRound parses {round}: "1", "2" or "3".
func (s *Server) pathRound(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.PathValue("round")
	if len(v) != 1 || v[0] < '1' || v[0] > '0'+game.Rounds {
		s.malformed(w, "round must be 1, 2 or 3")
		return 0, false
	}
	return int(v[0] - '0'), true
}

func validJurisdiction(j string) bool {
	if len(j) < 2 || len(j) > 8 {
		return false
	}
	for i := 0; i < len(j); i++ {
		if j[i] < 'A' || j[i] > 'Z' {
			return false
		}
	}
	return true
}

// answer writes the response a recorded result makes (section 7.2).
// success builds the status and body of an ok result from applied state.
func (s *Server) answer(w http.ResponseWriter, res tournament.Result, sequenced bool, success func(st *tournament.State) (int, any, error)) {
	h := w.Header()
	switch res.Code {
	case tournament.KeyReused:
		s.writeError(w, http.StatusUnprocessableEntity, CodeKeyReused, "the idempotency key was used with a different request", false)
		return
	case tournament.InvalidKey, tournament.InvalidOp, tournament.OutOfOrder:
		s.log.Error("unexpected result", "code", res.Code, "detail", res.Detail)
		s.writeError(w, http.StatusInternalServerError, CodeInternal, "unexpected result "+string(res.Code), true)
		return
	}
	setSlot(h, HeaderSlot, uint64(res.Slot))
	if sequenced && res.Play.NextSeq > 0 {
		setSlot(h, HeaderNextSeq, res.Play.NextSeq)
	}
	if res.Code != tournament.OK {
		setSlot(h, HeaderAppliedSlot, uint64(s.backend.Status().Applied))
		status := http.StatusConflict
		if res.Code == tournament.DeviceMismatch {
			status = http.StatusForbidden
		}
		s.writeError(w, status, string(res.Code), res.Detail, false)
		return
	}
	var (
		status  int
		body    any
		err     error
		applied uint64
	)
	if !s.readState(w, func(st *tournament.State) error {
		applied = uint64(st.Applied())
		status, body, err = success(st)
		return nil
	}) {
		return
	}
	if err != nil {
		s.log.Error("build response", "slot", res.Slot, "err", err)
		s.writeError(w, http.StatusInternalServerError, CodeInternal, "the response could not be built; retry the same request", true)
		return
	}
	setSlot(h, HeaderAppliedSlot, applied)
	s.writeJSON(w, status, body)
}

func setSlot(h http.Header, name string, v uint64) { h.Set(name, strconv.FormatUint(v, 10)) }

// beginIntent runs steps 2 to 5 of a sequenced POST: leader, key, token,
// rate limit.
func (s *Server) beginIntent(w http.ResponseWriter, r *http.Request) (session.Claims, string, bool) {
	if !s.onLeader(w, r) {
		return session.Claims{}, "", false
	}
	key, ok := s.clientKey(w, r)
	if !ok {
		return session.Claims{}, "", false
	}
	c, ok := s.authenticate(w, r)
	if !ok {
		return session.Claims{}, "", false
	}
	if !s.allow(w, s.intentLimit, c.Player) {
		return session.Claims{}, "", false
	}
	return c, key, true
}

// finishIntent runs steps 8 and 9 of a sequenced POST and answers.
func (s *Server) finishIntent(w http.ResponseWriter, r *http.Request, c session.Claims, key string, op tournament.Op, success func(st *tournament.State, res tournament.Result) (int, any, error)) {
	nkey := tournament.IdempotencyKey(PlayerKeyPrefix + c.Player + "." + key)
	if !s.lock(w, nkey) {
		return
	}
	defer s.unlock(nkey)
	res, ok := s.submit(w, r, tournament.Command{Key: nkey, ReceivedAt: s.cfg.Now().UnixMilli(), Op: op})
	if !ok {
		return
	}
	s.answer(w, res, true, func(st *tournament.State) (int, any, error) { return success(st, res) })
}

// seqBody reads a SeqRequest.
func (s *Server) seqBody(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	var req SeqRequest
	if !s.readBody(w, r, &req) {
		return 0, false
	}
	if req.Seq < 1 {
		s.malformed(w, "seq must be at least 1")
		return 0, false
	}
	return uint64(req.Seq), true
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if !s.onLeader(w, r) {
		return
	}
	key, ok := s.clientKey(w, r)
	if !ok {
		return
	}
	if !s.allow(w, s.sessionAddr, s.clientAddr(r)) {
		return
	}
	var req SessionRequest
	if !s.readBody(w, r, &req) {
		return
	}
	verifier, err := session.NewVerifier(req.DeviceSecret)
	switch {
	case !session.ValidDeviceID(req.DeviceID):
		s.malformed(w, "device_id must be 32 lower-case hex characters")
		return
	case err != nil:
		s.malformed(w, "device_secret must be 64 lower-case hex characters")
		return
	case !validJurisdiction(req.Jurisdiction):
		s.malformed(w, "jurisdiction must be 2 to 8 upper-case letters")
		return
	case req.Age < 0 || req.Age > 150:
		s.malformed(w, "age must be 0 to 150")
		return
	}
	if !s.allow(w, s.sessionDevice, req.DeviceID) {
		return
	}
	device := tournament.DeviceID(req.DeviceID)
	var mismatch bool
	if !s.readState(w, func(st *tournament.State) error {
		b, bound := st.Binding(device)
		mismatch = bound && b.Verifier != tournament.Digest(verifier)
		return nil
	}) {
		return
	}
	if mismatch {
		s.writeError(w, http.StatusForbidden, string(tournament.DeviceMismatch), "the device secret does not match the device's binding", false)
		return
	}
	nkey := tournament.IdempotencyKey(DeviceKeyPrefix + req.DeviceID + "." + key)
	if !s.lock(w, nkey) {
		return
	}
	defer s.unlock(nkey)
	op := tournament.OpenSession{Device: device, Verifier: tournament.Digest(verifier), Player: s.cfg.NewPlayerID(),
		Jurisdiction: req.Jurisdiction, Age: int(req.Age)}
	res, ok := s.submit(w, r, tournament.Command{Key: nkey, ReceivedAt: s.cfg.Now().UnixMilli(), Op: op})
	if !ok {
		return
	}
	s.answer(w, res, false, func(st *tournament.State) (int, any, error) {
		b, bound := st.Binding(device)
		if !bound || b.Player != res.Play.Player {
			return 0, nil, errNoRecord
		}
		exp := res.Play.IssuedAtMs + s.cfg.SessionTTL.Milliseconds()
		tok, err := s.cfg.Keys.Sign(session.Claims{Player: string(b.Player), Device: req.DeviceID, IssuedAtMs: res.Play.IssuedAtMs, ExpiresAtMs: exp})
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, SessionResponse{
			Replayed: res.Replayed, Slot: int64(res.Slot), NextSeq: int64(res.Play.NextSeq),
			PlayerID: string(b.Player), NewPlayer: res.Play.NewPlayer, SessionToken: tok,
			IssuedAtMs: res.Play.IssuedAtMs, ExpiresAtMs: exp, Jurisdiction: b.Jurisdiction, Age: int32(b.Age),
		}, nil
	})
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	c, key, ok := s.beginIntent(w, r)
	if !ok {
		return
	}
	seq, ok := s.seqBody(w, r)
	if !ok {
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	pid := tournament.PlayerID(c.Player)
	s.finishIntent(w, r, c, key, tournament.Enter{Tournament: tid, Player: pid, Seq: seq},
		func(st *tournament.State, res tournament.Result) (int, any, error) {
			h, exists := st.Header(tid)
			if !exists {
				return 0, nil, errNoRecord
			}
			return http.StatusCreated, JoinResponse{
				Replayed: res.Replayed, Slot: int64(res.Slot), NextSeq: int64(res.Play.NextSeq),
				TournamentID: string(tid), JoinSeq: int32(res.Play.JoinSeq), EntryFee: int64(h.Rules.EntryFee), Rounds: game.Rounds,
			}, nil
		})
}

// roundResponse builds the body of the round routes as of res.
func roundResponse(status int, tid tournament.TournamentID, pid tournament.PlayerID, round int) func(st *tournament.State, res tournament.Result) (int, any, error) {
	return func(st *tournament.State, res tournament.Result) (int, any, error) {
		v, ok := BuildRoundViewAt(st, tid, pid, round, res.Play.MoveIndex, res.Slot)
		if !ok {
			return 0, nil, errNoRecord
		}
		return status, RoundResponse{Replayed: res.Replayed, Slot: int64(res.Slot), NextSeq: int64(res.Play.NextSeq), Round: v}, nil
	}
}

func (s *Server) handleDeal(w http.ResponseWriter, r *http.Request) {
	c, key, ok := s.beginIntent(w, r)
	if !ok {
		return
	}
	seq, ok := s.seqBody(w, r)
	if !ok {
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	round, ok := s.pathRound(w, r)
	if !ok {
		return
	}
	pid := tournament.PlayerID(c.Player)
	seed := game.DeriveSeed(s.cfg.DealSecret, string(tid), c.Player, round)
	s.finishIntent(w, r, c, key, tournament.StartRound{Tournament: tid, Player: pid, Seq: seq, Round: round, Seed: seed},
		roundResponse(http.StatusCreated, tid, pid, round))
}

func (s *Server) handleMove(w http.ResponseWriter, r *http.Request) {
	c, key, ok := s.beginIntent(w, r)
	if !ok {
		return
	}
	var req MoveRequest
	if !s.readBody(w, r, &req) {
		return
	}
	switch {
	case req.Seq < 1:
		s.malformed(w, "seq must be at least 1")
		return
	case req.MoveIndex < 0:
		s.malformed(w, "move_index must be at least 0")
		return
	case req.Kind == string(game.Play) && (req.Column < 0 || req.Column >= game.ColumnCount):
		s.malformed(w, "a play names column 0 to 6")
		return
	case req.Kind == string(game.Draw) && req.Column != game.NoColumn:
		s.malformed(w, "a draw names column -1")
		return
	case req.Kind != string(game.Play) && req.Kind != string(game.Draw):
		s.malformed(w, `kind must be "play" or "draw"`)
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	round, ok := s.pathRound(w, r)
	if !ok {
		return
	}
	pid := tournament.PlayerID(c.Player)
	op := tournament.PlayMove{Tournament: tid, Player: pid, Seq: uint64(req.Seq), Round: round, MoveIndex: int(req.MoveIndex),
		Move: game.Move{Kind: game.MoveKind(req.Kind), Column: int(req.Column)}}
	s.finishIntent(w, r, c, key, op, roundResponse(http.StatusOK, tid, pid, round))
}

func (s *Server) handleFinish(w http.ResponseWriter, r *http.Request) {
	c, key, ok := s.beginIntent(w, r)
	if !ok {
		return
	}
	seq, ok := s.seqBody(w, r)
	if !ok {
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	round, ok := s.pathRound(w, r)
	if !ok {
		return
	}
	pid := tournament.PlayerID(c.Player)
	s.finishIntent(w, r, c, key, tournament.FinishRound{Tournament: tid, Player: pid, Seq: seq, Round: round},
		roundResponse(http.StatusOK, tid, pid, round))
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	c, key, ok := s.beginIntent(w, r)
	if !ok {
		return
	}
	seq, ok := s.seqBody(w, r)
	if !ok {
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	pid := tournament.PlayerID(c.Player)
	s.finishIntent(w, r, c, key, tournament.ClaimPayout{Tournament: tid, Player: pid, Seq: seq},
		func(st *tournament.State, res tournament.Result) (int, any, error) {
			return http.StatusOK, ClaimResponse{
				Replayed: res.Replayed, Slot: int64(res.Slot), NextSeq: int64(res.Play.NextSeq),
				TournamentID: string(tid), Amount: int64(res.Play.Amount), PostingKey: string(ledger.ClaimKey(string(tid), c.Player)),
			}, nil
		})
}
