package host

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// apiFakeAgent is a session API provider whose agents confirm whatever
// execution settings they are handed, and accept interrupts without ending
// the turn, so a test decides when the turn or the agent goes.
type apiFakeAgent struct{}

func init() { agent.Register(apiFakeAgent{}) }

var apiFakeInitializations atomic.Int64

func (apiFakeAgent) Kind() agent.Kind   { return "apifake" }
func (apiFakeAgent) Name() string       { return "API fake" }
func (apiFakeAgent) Level() agent.Level { return agent.LevelPreview }
func (apiFakeAgent) Features() map[agent.Feature]agent.Support {
	return map[agent.Feature]agent.Support{agent.FeatureRun: agent.Yes, agent.FeaturePrompt: agent.Yes}
}
func (apiFakeAgent) Profiles() []agent.Profile               { return nil }
func (apiFakeAgent) SessionAPIFeatures() []string            { return nil }
func (apiFakeAgent) SessionAPIProfile() agent.Profile        { return agent.Profile{Kind: "apifake"} }
func (apiFakeAgent) SessionAPIModel(model string) string     { return model }
func (apiFakeAgent) SessionAPIMode(mode string) string       { return mode }
func (apiFakeAgent) SessionAPIEngineMode(mode string) string { return mode }

func (apiFakeAgent) Start(_ context.Context, o agent.StartOptions) (agent.Conn, error) {
	return &apiFakeConn{fakeConn: fakeConn{events: make(chan event.Event, 32)}, session: o.SessionID, cwd: o.Dir, interrupted: make(chan struct{}, 1)}, nil
}

type apiFakeConn struct {
	fakeConn
	session, cwd string
	interrupted  chan struct{}
}

func (c *apiFakeConn) InitializeExecution(_ context.Context, model, effort, mode string) (event.Init, error) {
	apiFakeInitializations.Add(1)
	return event.Init{SessionID: c.session, Cwd: c.cwd, Model: model, Effort: effort, Mode: mode}, nil
}

func (c *apiFakeConn) InterruptConfirmed(context.Context) error {
	select {
	case c.interrupted <- struct{}{}:
	default:
	}
	return nil
}

// apiFakeHost is a live host on the fake provider whose first agent the
// API initialized and confirmed.
func apiFakeHost(t *testing.T) *server {
	t.Helper()
	home := t.TempDir()
	t.Setenv("RUSH_HOME", home)
	conversation, _ := NewSessionID()
	operation, _ := NewSessionID()
	installation, _ := NewSessionID()
	owner := RuntimeOwnership{Version: "1", Installation: installation, Chat: "chat", Operation: operation}
	s := &server{cfg: Config{ID: "restart", Kind: "apifake", SessionID: conversation, Cwd: home, Model: "opus", Effort: "medium", PermissionMode: "auto",
		Account: agent.Profile{Kind: "apifake", Dir: home}, Meta: apiMetadata(owner)},
		clients: map[*conn]struct{}{}, pending: map[string]asked{}, quit: make(chan struct{})}
	t.Cleanup(func() {
		s.stopOnce.Do(func() { close(s.quit) })
		s.mu.Lock()
		c := s.detach()
		s.mu.Unlock()
		if c != nil {
			c.Close()
		}
	})
	s.api = newAPIControl(s)
	if err := os.MkdirAll(dir(s.cfg.ID), 0700); err != nil {
		t.Fatal(err)
	}
	params, _ := json.Marshal(RuntimeInitializeRequest{OperationID: operation, ConversationID: conversation, Metadata: owner, Cwd: home,
		Provider: "apifake", Settings: RuntimeSettings{Model: "opus", Effort: "medium", Mode: "auto"}})
	response := s.api.execute(context.Background(), APIRequest{Protocol: 1, RequestID: "init", Method: "initialize", Params: params})
	if !response.OK {
		t.Fatalf("initialize: %+v", response.Error)
	}
	if phase := apiPhase(s); phase != "ready" {
		t.Fatalf("after initialize phase = %q, want ready", phase)
	}
	return s
}

func apiPhase(s *server) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.api.state.Phase
}

// apiHostRestart replaces the agent the way the host does on a usage limit or
// an account swap: no API call is involved.
func apiHostRestart(t *testing.T, s *server) {
	t.Helper()
	s.mu.Lock()
	old := s.detach()
	s.mu.Unlock()
	old.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.start(); err != nil {
		t.Fatal(err)
	}
	s.publish()
}

func TestPublicHostRestartedAgentReturnsToReady(t *testing.T) {
	s := apiFakeHost(t)
	s.mu.Lock()
	before := s.api.state.Identity.AgentIncarnation
	s.mu.Unlock()
	initializations := apiFakeInitializations.Load()

	apiHostRestart(t, s)

	deadline := time.Now().Add(5 * time.Second)
	for apiPhase(s) != "ready" {
		if time.Now().After(deadline) {
			s.mu.Lock()
			state := s.api.state
			s.mu.Unlock()
			t.Fatalf("restarted agent stuck: phase %q, settings confirmed %v", state.Phase, state.SettingsConfirmed)
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.api.state.SettingsConfirmed || s.api.state.Identity.AgentIncarnation == before {
		t.Fatalf("state after restart: %+v", s.api.state)
	}
	if got := apiFakeInitializations.Load() - initializations; got != 1 {
		t.Fatalf("restart ran %d handshakes, want 1", got)
	}
}

func TestPublicInterruptCutOffByAgentRestartIsRejected(t *testing.T) {
	s := apiFakeHost(t)
	s.mu.Lock()
	s.info.State = "idle"
	s.publish()
	s.info.State = "working" // a turn begins
	s.publish()
	expected, turn := s.api.state.Identity, s.api.state.Turn.ID
	target := s.conn.(*apiFakeConn)
	s.mu.Unlock()
	operation, _ := NewSessionID()
	params, _ := json.Marshal(RuntimeInterruptRequest{OperationID: operation, Expected: expected, TurnID: turn})

	done := make(chan APIResponse, 1)
	go func() {
		done <- s.api.execute(context.Background(), APIRequest{Protocol: 1, RequestID: "interrupt", Method: "interrupt", Params: params})
	}()
	select {
	case <-target.interrupted:
	case response := <-done:
		t.Fatalf("interrupt returned before reaching the agent: %+v", response.Error)
	case <-time.After(5 * time.Second):
		t.Fatal("interrupt never reached the agent")
	}
	apiHostRestart(t, s)

	var response APIResponse
	select {
	case response = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("interrupt never returned")
	}
	if response.OK || response.Error == nil || response.Error.Outcome != "not_applied" {
		t.Fatalf("interrupt response = %+v", response)
	}
	s.mu.Lock()
	receipt, err := s.api.read(operation)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != "rejected" || receipt.Identity.AgentIncarnation != expected.AgentIncarnation {
		t.Fatalf("receipt = %+v, want rejected for agent %s", receipt, expected.AgentIncarnation)
	}
	if err := receipt.Validate(expected, operation, "interrupt"); err != nil {
		t.Fatal(err)
	}
}
