package replog

import (
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

func TestConfigValidate(t *testing.T) {
	base := func() Config { return DefaultConfig(1, []paxos.NodeID{1, 2, 3}) }
	cases := []struct {
		name    string
		mod     func(*Config)
		wantErr bool
	}{
		{"defaults", func(*Config) {}, false},
		{"zero timing fields take defaults", func(c *Config) {
			c.HeartbeatInterval, c.ElectionTimeoutMin, c.ElectionTimeoutMax = 0, 0, 0
			c.Window, c.LearnBatch, c.QueueLimit = 0, 0, 0
		}, false},
		{"lease disabled", func(c *Config) { c.LeaseDuration = 0 }, false},
		{"single node", func(c *Config) { c.Peers = []paxos.NodeID{1} }, false},
		{"zero self", func(c *Config) { c.Self = 0 }, true},
		{"self not in peers", func(c *Config) { c.Self = 9 }, true},
		{"empty peers", func(c *Config) { c.Peers = nil }, true},
		{"zero peer", func(c *Config) { c.Peers = []paxos.NodeID{1, 0} }, true},
		{"duplicate peer", func(c *Config) { c.Peers = []paxos.NodeID{1, 2, 2} }, true},
		{"negative heartbeat", func(c *Config) { c.HeartbeatInterval = -time.Millisecond }, true},
		{"election min below heartbeat", func(c *Config) { c.ElectionTimeoutMin = c.HeartbeatInterval / 2 }, true},
		{"election max below min", func(c *Config) { c.ElectionTimeoutMax = c.ElectionTimeoutMin - time.Millisecond }, true},
		{"lease above election min", func(c *Config) { c.LeaseDuration = c.ElectionTimeoutMin + time.Millisecond }, true},
		{"negative lease", func(c *Config) { c.LeaseDuration = -time.Millisecond }, true},
		{"negative window", func(c *Config) { c.Window = -1 }, true},
		{"negative learn batch", func(c *Config) { c.LearnBatch = -1 }, true},
		{"negative queue limit", func(c *Config) { c.QueueLimit = -1 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mod(&cfg)
			err := cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", err, tc.wantErr)
			}
			if _, err := New(cfg, NewMemStore(), newTestRNG()); (err != nil) != tc.wantErr {
				t.Fatalf("New() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

func TestNewRejectsNilDependencies(t *testing.T) {
	cfg := DefaultConfig(1, []paxos.NodeID{1})
	if _, err := New(cfg, nil, newTestRNG()); err == nil {
		t.Error("New with nil store returned no error")
	}
	if _, err := New(cfg, NewMemStore(), nil); err == nil {
		t.Error("New with nil rng returned no error")
	}
}

func TestRoleString(t *testing.T) {
	cases := map[Role]string{Follower: "follower", Candidate: "candidate", Leader: "leader", Role(7): "role(7)"}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", uint8(r), got, want)
		}
	}
}

func TestEntryNoOp(t *testing.T) {
	if !(Entry{}).NoOp() || !(Entry{Value: paxos.Value{}}).NoOp() {
		t.Error("empty and nil values must be no-ops")
	}
	if (Entry{Value: paxos.Value("x")}).NoOp() {
		t.Error("a non-empty value is not a no-op")
	}
}
