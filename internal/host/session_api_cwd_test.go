package host

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// The host follows its agent's cd into its config, but the provider process
// keeps the folder it was spawned in. An Init from that process still
// confirms readiness; one from an unrelated folder does not.
func TestPublicInitAfterCwdFollowKeepsAgentReady(t *testing.T) {
	conversation, _ := NewSessionID()
	for _, tc := range []struct {
		name  string
		cwd   string
		ready bool
	}{
		{"spawn folder", "/work/worktree", true},
		{"followed folder", "/work", true},
		{"unrelated folder", "/elsewhere", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &server{cfg: Config{Kind: "claude", Cwd: "/work", Model: "opus", Effort: "medium", PermissionMode: "auto"}, startCwd: "/work/worktree"}
			a := &apiControl{s: s, handshakeConfirmed: true}
			a.state.Identity.ConversationID = conversation
			a.onEvent(event.Init{SessionID: conversation, Cwd: tc.cwd, Model: "claude-opus-4-5", Effort: "medium", Mode: "auto"})
			if a.ready != tc.ready {
				t.Fatalf("ready = %v, want %v", a.ready, tc.ready)
			}
		})
	}
}

// The daemon still names the folder it created the host in after the host
// followed a cd; initializing the attached, ready agent answers its state.
func TestPublicInitializeAfterCwdFollowAcceptsSpawnFolder(t *testing.T) {
	conversation, _ := NewSessionID()
	installation, _ := NewSessionID()
	settings := RuntimeSettings{Model: "opus", Effort: "medium", Mode: "auto"}
	for _, tc := range []struct {
		name string
		cwd  string
		code string
	}{
		{"spawn folder", "/work/worktree", ""},
		{"followed folder", "/work", ""},
		{"unrelated folder", "/elsewhere", "conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RUSH_HOME", t.TempDir())
			operation, _ := NewSessionID()
			owner := RuntimeOwnership{Version: "1", Installation: installation, Chat: "chat", Operation: operation}
			s := &server{cfg: Config{ID: "followed", Kind: "claude", Cwd: "/work", Model: "opus", Effort: "medium", PermissionMode: "auto"},
				startCwd: "/work/worktree", conn: &fakeConn{events: make(chan event.Event)}}
			a := &apiControl{s: s, ready: true}
			a.state.Identity = RuntimeIdentity{ConversationID: conversation, Metadata: owner}
			a.state.Settings = settings
			params, _ := json.Marshal(RuntimeInitializeRequest{OperationID: operation, ConversationID: conversation, Metadata: owner,
				Cwd: tc.cwd, Provider: "claude", Settings: settings})
			response := a.initialize(context.Background(), APIRequest{Protocol: 1, RequestID: "r", Method: "initialize", Params: params})
			code := ""
			if response.Error != nil {
				code = response.Error.Code
			}
			if code != tc.code {
				t.Fatalf("error = %q, want %q", code, tc.code)
			}
		})
	}
}
