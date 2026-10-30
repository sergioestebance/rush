package host

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// liveMCPAgent is the fake agent with one more thing it can do: take its
// caller's MCP servers while it runs. Each set is logged to mcp.log beside
// its profile, one line of name=command pairs.
type liveMCPAgent struct{ fakeAgent }

func init() { agent.Register(liveMCPAgent{}) }

func (liveMCPAgent) Kind() agent.Kind { return "fakemcp" }

func (a liveMCPAgent) Start(ctx context.Context, o agent.StartOptions) (agent.Conn, error) {
	c, err := a.fakeAgent.Start(ctx, o)
	if err != nil {
		return nil, err
	}
	return &liveMCPConn{fakeConn: c.(*fakeConn), log: filepath.Join(o.Profile.Dir, "mcp.log")}, nil
}

type liveMCPConn struct {
	*fakeConn
	log string
}

func (c *liveMCPConn) SetMCP(servers []agent.MCPServer) error {
	if _, err := os.Stat(filepath.Join(filepath.Dir(c.log), "mcp.slow")); err == nil {
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(c.log), "mcp.fail")); err == nil {
		return errors.New("the server did not start")
	}
	f, err := os.OpenFile(c.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(mcpLine(servers) + "\n")
	return err
}

// mcpLine is servers as name=command pairs, in their order.
func mcpLine(servers []agent.MCPServer) string {
	var out []string
	for _, m := range servers {
		out = append(out, m.Name+"="+m.Command)
	}
	return strings.Join(out, " ")
}

// idleAgent starts a session of kind with a first message, answers the
// approval it asks, and waits until it is idle.
func idleAgent(t *testing.T, kind agent.Kind) (c *Client, home, id string) {
	t.Helper()
	home = filepath.Dir(setup(t))
	cfg, err := Spawn(Config{Owner: os.Getpid(), Kind: string(kind), Cwd: home, Account: agent.Profile{Kind: kind, Name: "fake", Dir: home}, Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	c, err = Dial(cfg.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop(); c.Close() })
	ap := next(t, c, func(ev any) bool { _, ok := ev.(event.Approval); return ok }).(event.Approval)
	if err := c.Allow(ap.ID, nil, false); err != nil {
		t.Fatal(err)
	}
	next(t, c, inState("idle"))
	return c, home, cfg.ID
}

// lines is the file's lines, waiting up to 5s for at least n.
func lines(t *testing.T, path string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b, _ := os.ReadFile(path)
		got := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(b) == 0 {
			got = nil
		}
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// An mcp op gives a running agent that takes servers live the session's
// whole set at once, with no restart. A name the session runs already is
// kept as it was, and an op that adds nothing new asks nothing of the agent.
func TestMCPOpAddsServersToARunningAgentLive(t *testing.T) {
	c, home, id := idleAgent(t, "fakemcp")
	if err := c.AddMCP([]agent.MCPServer{{Name: "threads", Command: "/run/threads"}}); err != nil {
		t.Fatal(err)
	}
	if got := lines(t, filepath.Join(home, "mcp.log"), 1); len(got) != 1 || got[0] != "threads=/run/threads" {
		t.Fatalf("mcp.log = %q", got)
	}
	if err := c.AddMCP([]agent.MCPServer{{Name: "threads", Command: "/run/other"}, {Name: "notes", Command: "/run/notes"}}); err != nil {
		t.Fatal(err)
	}
	if got := lines(t, filepath.Join(home, "mcp.log"), 2); len(got) != 2 || got[1] != "notes=/run/notes threads=/run/threads" {
		t.Fatalf("mcp.log = %q", got)
	}
	if err := c.AddMCP([]agent.MCPServer{{Name: "threads", Command: "/run/other"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := lines(t, filepath.Join(home, "mcp.log"), 3); len(got) != 2 {
		t.Fatalf("an op adding nothing new reached the agent: %q", got)
	}
	if got := lines(t, filepath.Join(home, "starts.log"), 1); len(got) != 1 || got[0] != "start" {
		t.Fatalf("starts.log = %q, want one start", got)
	}
	cfg, err := ReadConfig(id)
	if err != nil {
		t.Fatal(err)
	}
	if got := mcpLine(cfg.MCP); got != "notes=/run/notes threads=/run/threads" {
		t.Fatalf("the session keeps %q", got)
	}
}

// An idle agent that cannot take servers live starts again on its
// conversation with them.
func TestMCPOpRestartsAnIdleAgentThatCannotTakeThemLive(t *testing.T) {
	c, home, _ := idleAgent(t, "fake")
	if err := c.AddMCP([]agent.MCPServer{{Name: "threads", Command: "/run/threads"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Send("again"); err != nil {
		t.Fatal(err)
	}
	got := lines(t, filepath.Join(home, "starts.log"), 2)
	if len(got) != 2 || got[1] != "start fake-session mcp=threads=/run/threads" {
		t.Fatalf("starts.log = %q, want a resume with the server", got)
	}
}

// A live add a busy agent refuses answers the op's id with its reason, so
// the caller can try again; the next op naming the same server hands the
// agent the set again, though the session keeps it already.
func TestMCPOpThatABusyAgentRefusesAnswersAnErrorAndIsTriedAgain(t *testing.T) {
	c, home, _ := idleAgent(t, "fakemcp")
	if err := c.Send("again"); err != nil {
		t.Fatal(err)
	}
	next(t, c, inState("blocked"))
	fail := filepath.Join(home, "mcp.fail")
	if err := os.WriteFile(fail, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	add := []byte(`{"op":"mcp","id":"r1","mcp":[{"name":"threads","command":"/run/threads"}]}`)
	if err := c.Op(add); err != nil {
		t.Fatal(err)
	}
	r := next(t, c, func(ev any) bool { r, ok := ev.(Reply); return ok && r.ID == "r1" }).(Reply)
	if !strings.Contains(r.Error, "did not start") {
		t.Fatalf("reply = %+v, want the agent's reason", r)
	}
	os.Remove(fail)
	if err := c.Op([]byte(`{"op":"mcp","id":"r2","mcp":[{"name":"threads","command":"/run/threads"}]}`)); err != nil {
		t.Fatal(err)
	}
	if r := next(t, c, func(ev any) bool { r, ok := ev.(Reply); return ok && r.ID == "r2" }).(Reply); r.Error != "" {
		t.Fatalf("the retry answered %q", r.Error)
	}
	if got := lines(t, filepath.Join(home, "mcp.log"), 1); len(got) != 1 || got[0] != "threads=/run/threads" {
		t.Fatalf("mcp.log = %q, want the set handed again", got)
	}
	if got := lines(t, filepath.Join(home, "starts.log"), 1); len(got) != 1 {
		t.Fatalf("starts.log = %q, want no restart", got)
	}
}

// Two mcp ops at once for the same server, from two clients, while the
// busy agent refuses it live: the second waits for the first and tries
// again itself, so neither answers that the agent has it.
func TestMCPOpsAtOnceNeverAnswerTakenWhileALiveAddFails(t *testing.T) {
	c, home, id := idleAgent(t, "fakemcp")
	if err := c.Send("again"); err != nil {
		t.Fatal(err)
	}
	next(t, c, inState("blocked"))
	for _, f := range []string{"mcp.fail", "mcp.slow"} {
		if err := os.WriteFile(filepath.Join(home, f), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c2, err := Dial(id)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if err := c.Op([]byte(`{"op":"mcp","id":"r1","mcp":[{"name":"threads","command":"/run/threads"}]}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond) // the first is in its live add
	if err := c2.Op([]byte(`{"op":"mcp","id":"r2","mcp":[{"name":"threads","command":"/run/threads"}]}`)); err != nil {
		t.Fatal(err)
	}
	if r := next(t, c2, func(ev any) bool { r, ok := ev.(Reply); return ok && r.ID == "r2" }).(Reply); r.Error == "" {
		t.Fatal("the second op answered taken while the agent refused the server")
	}
	if r := next(t, c, func(ev any) bool { r, ok := ev.(Reply); return ok && r.ID == "r1" }).(Reply); r.Error == "" {
		t.Fatal("the first op answered taken while the agent refused the server")
	}
}

// A host from before mcp ops is restarted on this rush with the servers
// added to the session's own, and its agent starts again with them.
func TestRestartWithMCPStartsTheHostAgainWithTheServers(t *testing.T) {
	home := filepath.Dir(setup(t))
	cfg, err := Spawn(Config{Kind: "fake", Cwd: home, Account: agent.Profile{Kind: "fake", Name: "fake", Dir: home},
		MCP: []agent.MCPServer{{Name: "notes", Command: "/run/notes"}}})
	if err != nil {
		t.Fatal(err)
	}
	old, _ := ReadInfo(cfg.ID)
	if err := RestartWithMCP(cfg.ID, []agent.MCPServer{{Name: "threads", Command: "/run/threads"}, {Name: "notes", Command: "/run/other"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c, err := Dial(cfg.ID); err == nil {
			c.Stop()
			c.Close()
		}
	})
	info, _ := ReadInfo(cfg.ID)
	if info.HostPID == old.HostPID || !alive(info.HostPID) || !info.TakesMCP() {
		t.Fatalf("restarted: %+v (was pid %d)", info, old.HostPID)
	}
	got, _ := ReadConfig(cfg.ID)
	if l := mcpLine(got.MCP); l != "notes=/run/notes threads=/run/threads" {
		t.Fatalf("the session keeps %q", l)
	}
}

// fakeSession starts a session of the fake agent running servers, stopped
// when the test ends.
func fakeSession(t *testing.T, servers ...agent.MCPServer) Config {
	t.Helper()
	home := filepath.Dir(setup(t))
	cfg, err := Spawn(Config{Kind: "fake", Cwd: home, Account: agent.Profile{Kind: "fake", Name: "fake", Dir: home}, MCP: servers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c, err := Dial(cfg.ID); err == nil {
			c.Stop()
			c.Close()
		}
	})
	return cfg
}

// A host whose session runs every server named already is left as it is:
// nothing to add is no reason to stop what it does.
func TestRestartWithMCPLeavesAHostThatRunsTheServers(t *testing.T) {
	cfg := fakeSession(t, agent.MCPServer{Name: "threads", Command: "/run/threads"})
	old, _ := ReadInfo(cfg.ID)
	if err := RestartWithMCP(cfg.ID, []agent.MCPServer{{Name: "threads", Command: "/run/other"}}); err != nil {
		t.Fatal(err)
	}
	if info, _ := ReadInfo(cfg.ID); info.HostPID != old.HostPID || !alive(old.HostPID) {
		t.Fatalf("restarted: pid %d, was %d", info.HostPID, old.HostPID)
	}
}

// A restart for MCP servers holds the session's wake lock, as a wake does,
// so a send that wakes the session meanwhile waits for the new host rather
// than starting a second one.
func TestRestartWithMCPWaitsForTheWakeLock(t *testing.T) {
	cfg := fakeSession(t)
	old, _ := ReadInfo(cfg.ID)
	unlock, err := lockWake(cfg.ID)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- RestartWithMCP(cfg.ID, []agent.MCPServer{{Name: "threads", Command: "/run/threads"}}) }()
	// A restart takes about 0.1 s, and about 1 s under -race.
	time.Sleep(3 * time.Second)
	if info, _ := ReadInfo(cfg.ID); info.HostPID != old.HostPID || !alive(old.HostPID) {
		t.Fatalf("restarted while a wake held the lock: pid %d, was %d", info.HostPID, old.HostPID)
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the restart never took the lock")
	}
	if info, _ := ReadInfo(cfg.ID); info.HostPID == old.HostPID || !alive(info.HostPID) {
		t.Fatalf("not restarted after the lock: %+v", info)
	}
}
