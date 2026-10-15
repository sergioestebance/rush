package host

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// stalledConn is an agent that never confirms it's ready: its initialization
// waits on the context and nothing else.
type stalledConn struct {
	fakeConn
	closed chan struct{}
	once   sync.Once
}

func (c *stalledConn) InitializeExecution(ctx context.Context, _, _, _ string) (event.Init, error) {
	<-ctx.Done()
	return event.Init{}, ctx.Err()
}

func (c *stalledConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.fakeConn.Close()
}

// An agent that doesn't confirm by the deadline leaves the host failed, saying
// so and by when, with the agent retired: no owner waits on it past that.
func TestPublicStartDeadlineFailsTheHost(t *testing.T) {
	t.Setenv("RUSH_HOME", t.TempDir())
	was := apiReadyWithin
	apiReadyWithin = 50 * time.Millisecond
	t.Cleanup(func() { apiReadyWithin = was })

	conversation, _ := NewSessionID()
	operation, _ := NewSessionID()
	stalled := &stalledConn{fakeConn: fakeConn{events: make(chan event.Event)}, closed: make(chan struct{})}
	s := &server{cfg: Config{ID: "stalled", Kind: "claude", Cwd: "/work", Model: "opus", Effort: "medium", PermissionMode: "auto"},
		quit: make(chan struct{}), clients: map[*conn]struct{}{}, pending: map[string]asked{}}
	s.api = newAPIControl(s)
	s.api.state.Identity.ConversationID = conversation
	s.conn = agent.Conn(stalled)
	s.api.agentStarted()

	s.mu.Lock()
	if err := s.api.begin(operation, "initialize", []byte(`{}`)); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.api.startConfirmation(operation, "initialize")
	s.publish()
	starting := s.api.state
	s.mu.Unlock()
	if starting.Phase != "agent_starting" || starting.Deadline.IsZero() || starting.PhaseSince.IsZero() || starting.Deadline.Sub(starting.PhaseSince) != apiReadyWithin {
		t.Fatalf("starting: %+v", starting)
	}

	select {
	case <-stalled.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled agent was never retired")
	}
	var failed RuntimeState
	for deadline := time.Now().Add(5 * time.Second); ; {
		s.mu.Lock()
		failed = s.api.state
		s.mu.Unlock()
		if failed.Phase == "failed" || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failed.Phase != "failed" || failed.Failure == nil || failed.Failure.Code != "timeout" || failed.Failure.Retry != "user" || !failed.Deadline.IsZero() {
		t.Fatalf("past its deadline: %+v %+v", failed, failed.Failure)
	}
	if receipt, err := s.api.read(operation); err != nil || receipt.Outcome != "rejected" {
		t.Fatalf("receipt: %+v %v", receipt, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil || s.api.state.Identity.AgentIncarnation != "" {
		t.Fatal("the failed host kept its agent")
	}
}

// The folder a host would start its agent in is checked first: gone, the
// start is refused outright, and the host failed with that reason.
func TestPublicStartRefusesAMissingFolder(t *testing.T) {
	t.Setenv("RUSH_HOME", t.TempDir())
	s := &server{cfg: Config{ID: "gone", Kind: "claude", Cwd: t.TempDir() + "/deleted", Model: "opus", Effort: "medium", PermissionMode: "auto"},
		quit: make(chan struct{}), clients: map[*conn]struct{}{}, pending: map[string]asked{}}
	s.api = newAPIControl(s)
	s.mu.Lock()
	defer s.mu.Unlock()
	if code, ok := s.api.startAgent(); ok || code != "folder_missing" {
		t.Fatalf("started in a missing folder: %q %v", code, ok)
	}
	s.publish()
	if state := s.api.state; state.Phase != "failed" || state.Failure == nil || state.Failure.Code != "folder_missing" {
		t.Fatalf("failed: %+v %+v", state, state.Failure)
	}
}
