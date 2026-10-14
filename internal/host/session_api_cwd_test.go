package host

import (
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
