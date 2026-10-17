package claude

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// A background agent's end, which Claude Code only queues, reads back as
// its task done: queued then taken it is told once.
func TestHistoryEndsBackgroundTasks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.jsonl")
	notice := `<task-notification>\n<task-id>a66</task-id>\n<tool-use-id>toolu_1</tool-use-id>\n<output-file>/o</output-file>\n<status>completed</status>\n<summary>Agent \"look\" finished</summary>\n</task-notification>`
	lines := []string{
		`{"type":"assistant","timestamp":"2026-09-29T10:00:00Z","uuid":"a1","message":{"id":"m1","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Agent","input":{"description":"look","prompt":"p","run_in_background":true}}]}}`,
		`{"type":"user","timestamp":"2026-09-29T10:00:01Z","uuid":"u1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"Async agent launched"}]},"toolUseResult":{"isAsync":true,"status":"async_launched","agentId":"a66"}}`,
		`{"type":"queue-operation","operation":"enqueue","timestamp":"2026-09-29T10:05:00Z","content":"` + notice + `"}`,
		`{"type":"queue-operation","operation":"remove","timestamp":"2026-09-29T10:05:01Z","content":"` + notice + `"}`,
		`{"type":"user","timestamp":"2026-09-29T10:05:02Z","uuid":"u2","message":{"role":"user","content":"` + notice + `"}}`,
	}
	must(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644))

	evs, err := Adapter{}.History(agent.Session{Transcript: path}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var done []event.TaskDone
	for _, ev := range evs {
		if d, ok := ev.(event.TaskDone); ok {
			done = append(done, d)
		}
	}
	want := event.TaskDone{ID: "a66", CallID: "toolu_1", Status: "completed", OutputFile: "/o", Summary: `Agent "look" finished`}
	if len(done) != 1 || done[0] != want {
		t.Errorf("done = %+v", done)
	}
}
