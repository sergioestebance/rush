package host

import (
	"os"
	"reflect"
	"testing"

	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// A session's commands outlive its agent: the host keeps the last list in
// the session's folder, and a host started again replays it.
func TestCommandsAreSavedForTheNextHost(t *testing.T) {
	setup(t)
	if err := os.MkdirAll(dir("cm"), 0o700); err != nil {
		t.Fatal(err)
	}
	list := []event.Command{{Name: "compact", Description: "Keep a summary", ArgumentHint: "[focus]", Aliases: []string{"c"}}}
	s := &server{cfg: Config{ID: "cm", Kind: "claude"}, clients: map[*conn]struct{}{}, pending: map[string]asked{}}
	s.mu.Lock()
	s.onAgentEvent(nil, event.Commands{List: list})
	s.mu.Unlock()
	if got := ReadCommands("cm"); !reflect.DeepEqual(got, list) {
		t.Fatalf("saved %+v, want %+v", got, list)
	}
	again := &server{cfg: Config{ID: "cm"}}
	again.loadCommands()
	v, err := Decode(again.commands)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := v.(Commands); !ok || !reflect.DeepEqual(c.Commands, list) {
		t.Fatalf("a new host replays %+v", v)
	}
	if ReadCommands("none") != nil {
		t.Fatal("a session with no list has commands")
	}
}
