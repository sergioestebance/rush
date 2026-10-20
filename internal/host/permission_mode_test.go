package host

import (
	"errors"
	"testing"

	"github.com/0xdeafcafe/rush/internal/agent/event"
)

type permissionConn struct {
	fakeConn
	fail   bool
	mode   string
	before func()
}

func (c *permissionConn) SetMode(mode string) error {
	if c.before != nil {
		c.before()
	}
	if c.fail {
		return errors.New("adapter rejected permissions")
	}
	c.mode = mode
	return nil
}
func TestPermissionModeCommitsOnlyAfterAcceptance(t *testing.T) {
	setup(t)
	c := &permissionConn{fail: true}
	s := &server{cfg: Config{ID: "permissions", Kind: "fake", PermissionMode: "ask"}, conn: c,
		info: Info{PermissionMode: "ask", State: "working", Queue: []string{"keep this"}, PermissionModes: []event.PermissionMode{{ID: "ask"}, {ID: "bypass"}}}}
	c.before = func() {
		if s.info.PermissionMode != "ask" || s.cfg.PermissionMode != "ask" {
			t.Fatal("mode was published before acceptance")
		}
	}
	if err := s.do(op{Op: "mode", Mode: "bypass"}); err == nil {
		t.Fatal("expected rejection")
	}
	if s.info.PermissionMode != "ask" || s.cfg.PermissionMode != "ask" {
		t.Fatal("rejection changed permissions")
	}
	c.fail = false
	if err := s.do(op{Op: "mode", Mode: "bypass"}); err != nil {
		t.Fatal(err)
	}
	if c.mode != "bypass" || s.info.PermissionMode != "bypass" || s.cfg.PermissionMode != "bypass" {
		t.Fatal("accepted permission mode not recorded")
	}
	if s.conn != c || s.info.State != "working" || len(s.info.Queue) != 1 {
		t.Fatal("permission change restarted or consumed work")
	}
	if err := s.do(op{Op: "mode", Mode: "invented"}); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// A send that names a mode goes in it: the mode first, then the message,
// as one op. A mode the agent refuses keeps the message back too.
func TestSendNamesItsMode(t *testing.T) {
	setup(t)
	c := &permissionConn{}
	s := &server{cfg: Config{ID: "sendmode", Kind: "fake", PermissionMode: "ask"}, conn: c, clients: map[*conn]struct{}{},
		info: Info{PermissionMode: "ask", State: "working", PermissionModes: []event.PermissionMode{{ID: "ask"}, {ID: "bypass"}}}}
	if err := s.do(op{Op: "send", Text: "go on", Mode: "bypass"}); err != nil {
		t.Fatal(err)
	}
	if c.mode != "bypass" || s.cfg.PermissionMode != "bypass" || len(s.info.Queue) != 1 {
		t.Fatalf("mode %q cfg %q queue %q", c.mode, s.cfg.PermissionMode, s.info.Queue)
	}
	if err := s.do(op{Op: "send", Text: "not this", Mode: "invented"}); err == nil {
		t.Fatal("unknown mode accepted")
	}
	c.fail = true
	if err := s.do(op{Op: "send", Text: "nor this", Mode: "ask"}); err == nil {
		t.Fatal("refused mode accepted")
	}
	if len(s.info.Queue) != 1 || s.cfg.PermissionMode != "bypass" {
		t.Fatalf("a refused mode still sent: queue %q mode %q", s.info.Queue, s.cfg.PermissionMode)
	}
	s.info.Sleeping = true
	if err := s.do(op{Op: "send", Text: "asleep", Mode: "ask"}); err == nil {
		t.Fatal("a sleeping session took a mode")
	}
}
