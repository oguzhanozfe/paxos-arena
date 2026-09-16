// Package intent serves the play API of docs/UNITY-INTEGRATION.md: the
// versioned HTTP routes a game client uses to open a session, list and
// enter tournaments, deal, move and finish rounds, read leaderboards, claim
// payouts and follow events.
//
// A client sends intents; the cluster decides outcomes. Every mutating
// route authenticates the session token with package session, namespaces
// the client's Idempotency-Key by player, and submits one tournament
// command through the replicated log; a follower answers it with 307 and
// the leader's address instead of forwarding it. Responses are built from
// applied state only, so no card is shown before the command that dealt it
// was chosen and applied, and a repeated key is answered with the response
// its first application produced. Every body obeys the JSON rules of the
// contract's section 8, which a JsonUtility client can read: fixed fields,
// no maps, no nulls, no top-level arrays.
//
// The routes are served on a listener of their own (arena's -play-listen),
// apart from the operator routes of package api, which stay on the replicas'
// internal network.
package intent

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// Backend is what the play API needs from a replica. replica.Runner
// provides Submit, Read and Status, as for package api; WaitApplied is the
// method the event stream adds to it.
type Backend interface {
	// Submit proposes cmd and waits for its result.
	Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error)
	// Read runs fn against the state, after a read-index barrier when
	// consistent is set.
	Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error
	// Status returns the replica's snapshot.
	Status() replica.Status
	// WaitApplied returns the applied slot as soon as it exceeds after, or
	// ctx's error when ctx ends first.
	WaitApplied(ctx context.Context, after paxos.Slot) (paxos.Slot, error)
}

// Rate is a token bucket: one token every Every, at most Burst held. A Rate
// whose Burst is negative does not limit.
type Rate struct {
	// Every is the refill interval of one token.
	Every time.Duration
	// Burst is the bucket's capacity.
	Burst int
}

// Limits are the per-replica rate limits of section 7.1.6. Buckets are local
// to a replica and not replicated; since followers redirect every intent,
// the leader's buckets are the ones that count for writes.
type Limits struct {
	// SessionPerDevice limits POST /v1/session per device id.
	SessionPerDevice Rate
	// SessionPerAddr limits POST /v1/session per client IP address.
	SessionPerAddr Rate
	// Intents limits the sequenced POST routes per player.
	Intents Rate
	// Reads limits the GET routes other than events per player.
	Reads Rate
	// EventPolls limits GET /v1/events per player.
	EventPolls Rate
}

// DefaultLimits are the limits of section 7.1.6, used when Config.Limits is
// zero.
var DefaultLimits = Limits{
	SessionPerDevice: Rate{Every: 10 * time.Second, Burst: 6},
	SessionPerAddr:   Rate{Every: time.Second, Burst: 30},
	Intents:          Rate{Every: 100 * time.Millisecond, Burst: 20},
	Reads:            Rate{Every: 200 * time.Millisecond, Burst: 10},
	EventPolls:       Rate{Every: 500 * time.Millisecond, Burst: 4},
}

// Config configures a Server.
type Config struct {
	// Self is this replica.
	Self paxos.NodeID
	// PublicURLs maps every node to the base URL at which clients reach its
	// play listener; Location and X-Arena-Leader use it.
	PublicURLs map[paxos.NodeID]string
	// Keys signs and verifies session tokens.
	Keys session.Keyring
	// SessionTTL is the lifetime of an issued token. Default
	// session.DefaultTTL.
	SessionTTL time.Duration
	// DealSecret keys the derivation of deal seeds; at least
	// MinDealSecretBytes, the same on every replica.
	DealSecret []byte
	// RequestTimeout bounds the wait for a command to be applied. Default
	// 5s.
	RequestTimeout time.Duration
	// MaxBody bounds a request body. Default DefaultMaxBody.
	MaxBody int64
	// MinSlotWait bounds how long a read waits for this replica to apply
	// the slot named by min_slot. Default DefaultMinSlotWait.
	MinSlotWait time.Duration
	// Limits are the rate limits. Default DefaultLimits.
	Limits Limits
	// Now stamps receipt times and reads token expiry. Default time.Now.
	Now func() time.Time
	// NewPlayerID draws the identifier a new binding receives. Default:
	// PlayerIDPrefix and PlayerIDRandomLen lower-case base32 characters from
	// crypto/rand.
	NewPlayerID func() tournament.PlayerID
	// TrustedProxies lists the peers whose first X-Forwarded-For address is
	// taken as the client's address for the session-address rate limit.
	TrustedProxies []netip.Addr
	// MaxEventPolls bounds the events requests open at once on this replica,
	// one per player; a request beyond it answers 503 unavailable. Default
	// DefaultMaxEventPolls.
	MaxEventPolls int
}

// Server holds the play API's handlers.
type Server struct {
	cfg     Config
	backend Backend
	log     *slog.Logger
	proxies map[netip.Addr]bool

	sessionDevice *limiter
	sessionAddr   *limiter
	intentLimit   *limiter
	readLimit     *limiter
	pollLimit     *limiter

	mu sync.Mutex
	// inflight holds the namespaced keys being submitted on this replica.
	inflight map[tournament.IdempotencyKey]struct{}
	// polls holds each player's open events request on this replica.
	polls map[tournament.PlayerID]*poll
	// scanning is closed when the scan of open events requests in progress
	// ends; nil when none runs.
	scanning chan struct{}
}

// Routes, as net/http patterns.
const (
	RouteSession     = "POST /v1/session"
	RouteTournaments = "GET /v1/tournaments"
	RouteJoin        = "POST /v1/tournaments/{tournament_id}/join"
	RouteDeal        = "POST /v1/tournaments/{tournament_id}/rounds/{round}/deal"
	RouteRound       = "GET /v1/tournaments/{tournament_id}/rounds/{round}"
	RouteMove        = "POST /v1/tournaments/{tournament_id}/rounds/{round}/moves"
	RouteFinish      = "POST /v1/tournaments/{tournament_id}/rounds/{round}/finish"
	RouteLeaderboard = "GET /v1/tournaments/{tournament_id}/leaderboard"
	RouteClaim       = "POST /v1/tournaments/{tournament_id}/payout/claim"
	RouteEvents      = "GET /v1/events"
)

// Header names.
const (
	HeaderAuthorization  = "Authorization"
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderLocation       = "Location"
	HeaderRetryAfter     = "Retry-After"
	HeaderServerTime     = "X-Arena-Server-Time-Ms"
	HeaderNode           = "X-Arena-Node"
	HeaderLeader         = "X-Arena-Leader"
	HeaderAppliedSlot    = "X-Arena-Applied-Slot"
	HeaderSlot           = "X-Arena-Slot"
	HeaderNextSeq        = "X-Arena-Next-Seq"
)

// Query parameters.
const (
	QueryStatus       = "status"
	QueryOffset       = "offset"
	QueryLimit        = "limit"
	QueryMinSlot      = "min_slot"
	QueryCursor       = "cursor"
	QueryWaitMs       = "wait_ms"
	QueryTournamentID = "tournament_id"
)

// Error codes the play API answers outside the state machine (section 7.4).
// A rejection by the state machine carries its tournament.Code, and a read
// of a tournament, entry or round that does not exist answers 404 with
// tournament.UnknownTournament, tournament.NotJoined or
// tournament.RoundNotStarted.
const (
	CodeNotLeader        = "not_leader"
	CodeNoLeader         = "no_leader"
	CodeMissingKey       = "missing_idempotency_key"
	CodeInvalidKey       = "invalid_idempotency_key"
	CodeMalformed        = "malformed_request"
	CodeBodyTooLarge     = "body_too_large"
	CodeSessionMissing   = "session_missing"
	CodeSessionInvalid   = "session_invalid"
	CodeSessionExpired   = "session_expired"
	CodeNotFound         = "not_found"
	CodeMethodNotAllowed = "method_not_allowed"
	CodeInFlight         = "in_flight"
	CodeKeyReused        = "key_reused"
	CodeRateLimited      = "rate_limited"
	CodeInternal         = "internal"
	CodeReplicaBehind    = "replica_behind"
	CodeUnavailable      = "unavailable"
	CodeOutcomeUnknown   = "outcome_unknown"
)

// Bounds and defaults.
const (
	// DefaultMaxBody bounds a request body; the largest valid play request
	// is under 300 bytes.
	DefaultMaxBody = 4 << 10
	// DefaultMinSlotWait is the default of Config.MinSlotWait.
	DefaultMinSlotWait = time.Second
	// DefaultEventsWait is the wait of an events request without wait_ms.
	DefaultEventsWait = 25 * time.Second
	// MaxEventsWait bounds wait_ms. The play listener's write timeout is
	// longer than this.
	MaxEventsWait = 25 * time.Second
	// MaxEventsPerResponse bounds the events of one response.
	MaxEventsPerResponse = 100
	// DefaultMaxEventPolls is the default of Config.MaxEventPolls.
	DefaultMaxEventPolls = 10000
	// DefaultPageLimit is the page size of a list without limit.
	DefaultPageLimit = 50
	// MaxPageLimit bounds limit.
	MaxPageLimit = 100
	// MinClientKeyLen and MaxClientKeyLen bound an Idempotency-Key of the
	// play API: characters from A-Z, a-z, 0-9, '_' and '-'.
	MinClientKeyLen = 16
	MaxClientKeyLen = 64
	// PlayerKeyPrefix and DeviceKeyPrefix namespace a client key in the log:
	// "u.<player id>.<key>" for sequenced intents and "d.<device id>.<key>"
	// for sessions, so no client can use a key another player will send.
	PlayerKeyPrefix = "u."
	DeviceKeyPrefix = "d."
	// PlayerIDPrefix starts every player id the play API draws, followed by
	// PlayerIDRandomLen characters of lower-case base32 (a-z, 2-7).
	PlayerIDPrefix    = "p-"
	PlayerIDRandomLen = 26
	// MinDealSecretBytes is the shortest deal secret arena accepts.
	MinDealSecretBytes = 32
	// EnvDealSecret names the environment variable holding the deal secret
	// as hex.
	EnvDealSecret = "ARENA_DEAL_SECRET"
)
