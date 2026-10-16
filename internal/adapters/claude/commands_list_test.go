package claude

import (
	"reflect"
	"testing"

	"github.com/0xdeafcafe/rush/internal/adapters/claude/headless"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// The list a session gives leaves out what only Claude Code's own terminal
// can do: the commands init names as terminal-only, and the ones rush
// leaves out or does its own way. A changed list replaces it whole.
func TestCommandsLeaveOutTheTerminals(t *testing.T) {
	c := &conn{events: make(chan event.Event, 8), initID: "i1", waits: map[string]chan headless.ControlReply{}}
	names := func() []string {
		t.Helper()
		select {
		case ev := <-c.events:
			var out []string
			for _, k := range ev.(event.Commands).List {
				out = append(out, k.Name)
			}
			return out
		default:
			t.Fatal("no commands")
			return nil
		}
	}
	c.own(headless.ControlReply{ID: "i1", Body: []byte(`{"commands":[{"name":"compact"},{"name":"doctor"},{"name":"color"},{"name":"clear"},{"name":"model"},{"name":"review"}]}`)})
	if got := names(); !reflect.DeepEqual(got, []string{"compact", "doctor", "review"}) {
		t.Fatalf("after initialize: %q", got)
	}
	c.own(headless.Init{TerminalCommands: []string{"doctor"}})
	if got := names(); !reflect.DeepEqual(got, []string{"compact", "review"}) {
		t.Fatalf("after init: %q", got)
	}
	if !c.own(headless.CommandsChanged{Commands: []headless.Command{{Name: "compact"}, {Name: "doctor"}, {Name: "ship-notes"}}}) {
		t.Fatal("a changed list went on as an event of its own")
	}
	if got := names(); !reflect.DeepEqual(got, []string{"compact", "ship-notes"}) {
		t.Fatalf("after a change: %q", got)
	}
}

func TestDecodeCommandsChanged(t *testing.T) {
	ev, err := headless.Decode([]byte(`{"type":"system","subtype":"commands_changed","commands":[{"name":"lint","description":"Lint it","argumentHint":"[path]"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := headless.CommandsChanged{Commands: []headless.Command{{Name: "lint", Description: "Lint it", ArgumentHint: "[path]"}}}
	if !reflect.DeepEqual(ev, want) {
		t.Fatalf("got %#v", ev)
	}
	ev, _ = headless.Decode([]byte(`{"type":"system","subtype":"init","session_id":"s","terminal_slash_commands":["exit"]}`))
	if got := ev.(headless.Init).TerminalCommands; !reflect.DeepEqual(got, []string{"exit"}) {
		t.Fatalf("init's terminal commands: %q", got)
	}
}
