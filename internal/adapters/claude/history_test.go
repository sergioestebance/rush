package claude

import (
	"fmt"
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

// counted is a file that counts what's read of it.
type counted struct {
	f           *os.File
	read, calls int64
}

func (c *counted) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.f.ReadAt(p, off)
	c.read += int64(n)
	c.calls++
	return n, err
}

// A tail is read back from a transcript's end: the turns before a time,
// then its last bytes, and nothing near its start.
func TestHistoryTailReadsFromTheEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.jsonl")
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	var b strings.Builder
	for i := range 200 {
		fmt.Fprintf(&b, `{"type":"user","uuid":"u%d","message":{"role":"user","content":"q%d"},"timestamp":%q}`+"\n", i, i, at.Add(time.Duration(2*i)*time.Second).Format(time.RFC3339Nano))
		fmt.Fprintf(&b, `{"type":"assistant","uuid":"a%d","message":{"id":"m%d","role":"assistant","content":[{"type":"text","text":"a%d %s"}]},"timestamp":%q}`+"\n", i, i, i, strings.Repeat("w", 10<<10), at.Add(time.Duration(2*i+1)*time.Second).Format(time.RFC3339Nano))
	}
	must(t, os.WriteFile(path, []byte(b.String()), 0o644))
	s := agent.Session{Transcript: path}

	evs, cut, err := Adapter{}.HistoryTail(s, 64<<10)
	if err != nil || !cut || len(evs) == 0 {
		t.Fatalf("tail: %d events, cut %v, %v", len(evs), cut, err)
	}
	if last := evs[len(evs)-1].(event.Message); !strings.HasPrefix(last.Parts[0].Text, "a199 ") {
		t.Errorf("the tail ends with %.10s", last.Parts[0].Text)
	}
	evs, _, err = Adapter{}.HistoryTailBefore(s, 64<<10, at.Add(300*time.Second))
	if err != nil || len(evs) == 0 {
		t.Fatal(err)
	}
	if last := evs[len(evs)-1].(event.Message); !strings.HasPrefix(last.Parts[0].Text, "a149 ") {
		t.Errorf("before turn 150 ends with %.10s", last.Parts[0].Text)
	}

	f, err := os.Open(path)
	must(t, err)
	defer f.Close()
	c := &counted{f: f}
	size := int64(b.Len())
	start, end, cut, err := tailWindow(c, size, 64<<10, at.Add(360*time.Second))
	if err != nil || !cut || end >= size || start >= end {
		t.Fatalf("window %d..%d of %d, cut %v, %v", start, end, size, cut, err)
	}
	if c.read > size/2 {
		t.Errorf("read %d of %d bytes for a tail", c.read, size)
	}
}

// A long line is read back in growing reads, not one small read after
// another, each copying all read so far.
func TestHistoryTailReadsALongLineInFewReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.jsonl")
	line := `{"type":"assistant","message":{"id":"m","role":"assistant","content":[{"type":"text","text":"` + strings.Repeat("w", 4<<20) + `"}]},"timestamp":"2026-09-29T10:00:00Z"}` + "\n"
	must(t, os.WriteFile(path, []byte(line), 0o644))
	f, err := os.Open(path)
	must(t, err)
	defer f.Close()
	c := &counted{f: f}
	if _, _, _, err := tailWindow(c, int64(len(line)), 1<<20, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if c.calls > 10 {
		t.Errorf("%d reads for one line of %d bytes", c.calls, len(line))
	}
}
