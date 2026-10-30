package claude

import (
	"context"
	"encoding/json/jsontext"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// controlClaude answers rush's control requests as claude -p does: the
// initialize with its commands, then an MCP message and a permission check
// for rush's own tool, and any other request with its subtype echoed.
func controlClaude(t *testing.T) (bin, log string) {
	dir := t.TempDir()
	log = filepath.Join(dir, "in.log")
	script := `#!/bin/sh
printf '%s\n' "$*" > "` + log + `.args"
env > "` + log + `.env"
read -r init; printf '%s\n' "$init" >> "` + log + `"
echo '{"type":"control_response","response":{"subtype":"success","request_id":"rush-1","response":{"commands":[{"name":"review","description":"Review a PR","argumentHint":"<pr>","aliases":[]}]}}}'
echo '{"type":"control_request","request_id":"m-1","request":{"subtype":"mcp_message","server_name":"t","message":{"jsonrpc":"2.0","id":1,"method":"tools/list"}}}'
echo '{"type":"control_request","request_id":"p-1","request":{"subtype":"can_use_tool","tool_name":"mcp__t__draw","input":{},"tool_use_id":"tu-9"}}'
while read -r line; do
  printf '%s\n' "$line" >> "` + log + `"
  id=$(printf '%s' "$line" | sed -n 's/.*"request_id":"\(rush-[0-9]*\)".*/\1/p')
  sub=$(printf '%s' "$line" | sed -n 's/.*"subtype":"\([a-z_]*\)".*/\1/p')
  [ -n "$id" ] || continue
  case "$sub" in
  get_context_usage) echo '{"type":"control_response","response":{"subtype":"success","request_id":"'$id'","response":{"totalTokens":1200,"maxTokens":200000}}}' ;;
  mcp_set_servers) echo '{"type":"control_response","response":{"subtype":"success","request_id":"'$id'","response":{"added":["amtana-threads"],"removed":[],"errors":{}}}}'
    echo '{"type":"control_request","request_id":"p-2","request":{"subtype":"can_use_tool","tool_name":"mcp__amtana-threads__reply","input":{},"tool_use_id":"tu-2"}}' ;;
  *) echo '{"type":"control_response","response":{"subtype":"success","request_id":"'$id'","response":{"did":"'$sub'"}}}' ;;
  esac
done
`
	bin = filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, log
}

// wantIn fails unless the file at path has each of wants.
func wantIn(t *testing.T, path string, wants ...string) {
	t.Helper()
	b, _ := os.ReadFile(path)
	for _, w := range wants {
		if !strings.Contains(string(b), w) {
			t.Errorf("%s has %s\nmissing %s", filepath.Base(path), b, w)
		}
	}
}

// The conn answers rush's own tools itself, keeps their traffic off the
// tap, gives the session's commands as an event, and passes control
// requests through.
func TestConnControl(t *testing.T) {
	bin, log := controlClaude(t)
	var mu sync.Mutex
	var tapped []string
	handled := make(chan jsontext.Value, 1)
	tmp := filepath.Join(t.TempDir(), "tmp")
	conn, err := Adapter{}.Start(context.Background(), agent.StartOptions{Dir: t.TempDir(), Binary: bin, TempDir: tmp, Lean: true,
		Tools: []agent.ToolServer{{Name: "t", Trusted: []string{"draw"}, Handle: func(msg jsontext.Value) jsontext.Value {
			handled <- msg
			return jsontext.Value(`{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}`)
		}}},
		Agents: map[string]jsontext.Value{"p:helper": jsontext.Value(`{"description":"helps"}`)}, Prompt: "Be kind.",
		Tap: func(l []byte) { mu.Lock(); tapped = append(tapped, string(l)); mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case ev := <-conn.Events():
		c, ok := ev.(event.Commands)
		if !ok || len(c.List) != 1 || c.List[0].Name != "review" || c.List[0].ArgumentHint != "<pr>" {
			t.Fatalf("first event = %#v, want the commands", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no commands")
	}
	select {
	case msg := <-handled:
		if !strings.Contains(string(msg), "tools/list") {
			t.Errorf("handled %s", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the tool server wasn't asked")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, err := conn.(agent.Asker).Ask(ctx, jsontext.Value(`{"subtype":"get_settings"}`))
	if err != nil || !strings.Contains(string(body), `"did":"get_settings"`) {
		t.Fatalf("Ask = %s, %v", body, err)
	}
	u, err := conn.(agent.ContextReader).ContextUsage(ctx)
	if err != nil || u.Total != 1200 || u.At.IsZero() {
		t.Fatalf("ContextUsage = %+v, %v", u, err)
	}
	if err := conn.(agent.TaskStopper).StopTask("b1"); err != nil {
		t.Fatal(err)
	}
	if err := conn.(agent.Backgrounder).Background("tu-1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	_ = conn.Close()
	wantIn(t, log, `"sdkMcpServers":["t"]`, `"mcp_response"`, `"request_id":"p-1"`, `"behavior":"allow"`, `"subtype":"stop_task"`, `"task_id":"b1"`, `"tool_use_id":"tu-1"`)
	wantIn(t, log+".args", "--allowedTools mcp__t__draw", `--agents {"p:helper":{"description":"helps"}}`, "--append-system-prompt Be kind.")
	wantIn(t, log+".env", "CLAUDE_CODE_TMPDIR="+tmp, "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1", "CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING=1")
	mu.Lock()
	defer mu.Unlock()
	for _, l := range tapped {
		if strings.Contains(l, `"m-1"`) || strings.Contains(l, `"p-1"`) {
			t.Errorf("own traffic reached the tap: %s", l)
		}
	}
	if len(tapped) == 0 {
		t.Error("nothing reached the tap")
	}
}

// Answers go into the question's own input: what else Claude put there
// stays, and an answer's own keys win.
func TestAnswersKeepTheQuestion(t *testing.T) {
	got := string(over(jsontext.Value(`{"questions":[{"question":"Go?"}],"metadata":{"source":"x"},"answers":{}}`), jsontext.Value(`{"answers":{"Go?":"yes"}}`)))
	for _, want := range []string{`"questions":[{"question":"Go?"}]`, `"metadata":{"source":"x"}`, `"answers":{"Go?":"yes"}`} {
		if !strings.Contains(got, want) {
			t.Errorf("%s is missing %s", got, want)
		}
	}
}

// A caller's MCP servers reach Claude Code as one config beside rush's
// own, every tool of theirs trusted. With a scratch folder the config is a
// file only the user reads, so a server's environment stays off the
// command line.
func TestCallerMCPConfig(t *testing.T) {
	if f, err := mcpFlags(nil, ""); f != nil || err != nil {
		t.Fatalf("none: %v %v", f, err)
	}
	servers := []agent.MCPServer{{Name: "amtana-threads", Command: "/run/amtana", Args: []string{"threads-mcp"}}}
	f, err := mcpFlags(servers, "")
	want := []string{"--mcp-config", `{"mcpServers":{"amtana-threads":{"type":"stdio","command":"/run/amtana","args":["threads-mcp"]}}}`}
	if err != nil || !slices.Equal(f, want) {
		t.Fatalf("flags = %q %v", f, err)
	}
	tmp := filepath.Join(t.TempDir(), "tmp")
	servers[0].Env = map[string]string{"AMTANA_TOOL_TOKEN": "tok"}
	f, err = mcpFlags(servers, tmp)
	if err != nil || len(f) != 2 || f[0] != "--mcp-config" || strings.Contains(f[1], "tok") {
		t.Fatalf("flags = %q %v", f, err)
	}
	st, err := os.Stat(f[1])
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("config file %s: %v %v", f[1], st, err)
	}
	if b, _ := os.ReadFile(f[1]); !strings.Contains(string(b), `"env":{"AMTANA_TOOL_TOKEN":"tok"}`) {
		t.Fatalf("config file holds %s", b)
	}
	c := &conn{mcp: []agent.MCPServer{{Name: "amtana-threads"}}}
	if !slices.Contains(c.trusted(), "mcp__amtana-threads") {
		t.Fatalf("trusted = %v", c.trusted())
	}
}

// A caller's MCP servers reach a running Claude Code as one
// mcp_set_servers request with the whole set, each with its environment,
// and every tool of theirs is trusted from then on, though --allowedTools
// named none of them.
func TestSetMCPGivesClaudeCodeTheServersLive(t *testing.T) {
	bin, log := controlClaude(t)
	conn, err := Adapter{}.Start(context.Background(), agent.StartOptions{Dir: t.TempDir(), Binary: bin, TempDir: filepath.Join(t.TempDir(), "tmp"), Lean: true})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	setter, ok := conn.(agent.MCPSetter)
	if !ok {
		t.Fatal("Claude Code's conn takes no servers live")
	}
	servers := []agent.MCPServer{{Name: "amtana-threads", Command: "/run/threads-mcp", Env: map[string]string{"AMTANA_TOOL_TOKEN": "tok"}}}
	if err := setter.SetMCP(servers); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_ = conn.Close()
	wantIn(t, log, `"subtype":"mcp_set_servers"`, `"servers":{"amtana-threads":{"type":"stdio","command":"/run/threads-mcp","env":{"AMTANA_TOOL_TOKEN":"tok"}}}`)
	b, _ := os.ReadFile(log)
	allowed := false
	for _, l := range strings.Split(string(b), "\n") {
		allowed = allowed || strings.Contains(l, `"request_id":"p-2"`) && strings.Contains(l, `"behavior":"allow"`)
	}
	if !allowed {
		t.Errorf("the new server's tool was not allowed:\n%s", b)
	}
}
