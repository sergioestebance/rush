package claude

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/adapters/claude/headless"
	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// errNoTranscript is a session asked for its history that has no
// transcript to read it from.
var errNoTranscript = errors.New("claude: this session has no transcript")

// historyLine is what History needs of a transcript line.
type historyLine struct {
	Type          string         `json:"type"`
	IsMeta        bool           `json:"isMeta"`
	IsSidechain   bool           `json:"isSidechain"`
	Timestamp     time.Time      `json:"timestamp"`
	UUID          string         `json:"uuid"`
	Message       jsontext.Value `json:"message"`
	ToolUseResult jsontext.Value `json:"toolUseResult"`
	Operation     string         `json:"operation"`
	Content       jsontext.Value `json:"content"`
}

// History reads s's transcript back as rush's own events: its messages,
// from before before when that's set. A subagent's lines and Claude Code's
// own notes are left out, as convo leaves them out of a conversation.
func (Adapter) History(s agent.Session, before time.Time) ([]event.Event, error) { //nolint:gocritic // HistoryReader takes the session by value
	if s.Transcript == "" {
		return nil, errNoTranscript
	}
	f, err := os.Open(s.Transcript)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readHistory(f, before)
}

// HistoryTail is History of the transcript's last most bytes, from their
// first whole line on: cut is whether that left some out.
func (a Adapter) HistoryTail(s agent.Session, most int64) ([]event.Event, bool, error) { //nolint:gocritic // as History
	return a.HistoryTailBefore(s, most, time.Time{})
}

// HistoryTailBefore is HistoryTail of what was written before before
// (all of it when zero). The transcript is read back from its end, as far
// as before and then most bytes: never from its start, for a long one.
func (Adapter) HistoryTailBefore(s agent.Session, most int64, before time.Time) ([]event.Event, bool, error) { //nolint:gocritic // as History
	if s.Transcript == "" {
		return nil, false, errNoTranscript
	}
	f, err := os.Open(s.Transcript)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	start, end, cut, err := tailWindow(f, st.Size(), most, before)
	if err != nil {
		return nil, false, err
	}
	evs, err := readHistory(io.NewSectionReader(f, start, end-start), before)
	return evs, cut, err
}

// tailWindow is where the whole lines of f (size long) written before
// before end, and where the last most bytes of them start, though always
// with one line: lines are taken as written in time order. cut is whether
// there are lines before start.
func tailWindow(f io.ReaderAt, size, most int64, before time.Time) (start, end int64, cut bool, err error) {
	end = size
	if !before.IsZero() {
		end = 0
		err = linesBack(f, size, func(ls int64, line []byte) bool {
			var l struct {
				Timestamp time.Time `json:"timestamp"`
			}
			if jsonx.Unmarshal(line, &l) == nil && !l.Timestamp.IsZero() && l.Timestamp.Before(before) {
				end = min(size, ls+int64(len(line))+1)
				return false
			}
			return true
		})
		if err != nil {
			return 0, 0, false, err
		}
	}
	start = end
	err = linesBack(f, end, func(ls int64, _ []byte) bool {
		if start < end && end-ls > most {
			cut = true
			return false
		}
		start = ls
		return true
	})
	return start, end, cut, err
}

// linesBack calls each with f's lines that end by end, newest first, and
// where each starts, until each says stop. It holds a line at a time.
func linesBack(f io.ReaderAt, end int64, each func(start int64, line []byte) bool) error {
	const chunk = 64 << 10
	off, buf := end, []byte(nil) // buf is f[off:end]
	for end > 0 {
		var ls int64
		for {
			if i := bytes.LastIndexByte(buf[:max(0, end-1-off)], '\n'); i >= 0 {
				ls = off + int64(i) + 1
				break
			}
			if off == 0 {
				break
			}
			n := min(chunk, off)
			next := make([]byte, n+int64(len(buf)))
			if _, err := f.ReadAt(next[:n], off-n); err != nil && err != io.EOF {
				return err
			}
			copy(next[n:], buf)
			buf, off = next, off-n
		}
		if !each(ls, bytes.TrimSuffix(buf[ls-off:end-off], []byte{'\n'})) {
			return nil
		}
		buf, end = buf[:ls-off], ls
	}
	return nil
}

// readHistory is History read from f.
func readHistory(f io.Reader, before time.Time) ([]event.Event, error) {
	var (
		out  []event.Event
		n    headless.Neutral
		r    = jsonx.NewLineReader(f)
		told = map[string]bool{}
	)
	for {
		line, ok := r.Next()
		if !ok {
			break
		}
		var l historyLine
		if jsonx.Unmarshal(line, &l) != nil || l.IsMeta || l.IsSidechain {
			continue
		}
		if !before.IsZero() && !l.Timestamp.IsZero() && !l.Timestamp.Before(before) {
			break
		}
		if text, ok := notice(l); ok {
			for _, done := range ended(text) {
				key := done.ID + "," + done.CallID + "," + done.Status
				if !told[key] {
					told[key] = true
					out = append(out, done)
				}
			}
			continue
		}
		if l.Type != "user" && l.Type != "assistant" {
			continue
		}
		ev, err := headless.DecodeMessage(l.Type, l.Message, l.ToolUseResult)
		if err != nil {
			continue
		}
		if m, ok := ev.(headless.Message); ok {
			m.UUID = l.UUID
			ev = m
		}
		out = append(out, n.Event(ev)...)
	}
	return out, r.Err()
}

// notice is a line's word that a background task ended: Claude Code queues
// it as it comes, and only sometimes writes it as a user message too.
func notice(l historyLine) (string, bool) {
	var text string
	switch {
	case l.Type == "queue-operation" && l.Operation == "enqueue":
		if jsonx.Unmarshal(l.Content, &text) != nil {
			return "", false
		}
	case l.Type == "user":
		var m struct {
			Content string `json:"content"`
		}
		if jsonx.Unmarshal(l.Message, &m) != nil {
			return "", false
		}
		text = m.Content
	default:
		return "", false
	}
	text = strings.TrimSpace(text)
	return text, strings.HasPrefix(text, "<task-notification>")
}

// ended is the tasks a notice says ended: the history keeps no task events,
// so without it a background agent started before a restart runs forever.
func ended(s string) []event.TaskDone {
	var out []event.TaskDone
	for _, seg := range strings.Split(s, "<task-notification>")[1:] {
		seg, _, _ = strings.Cut(seg, "</task-notification>")
		status := tag(seg, "status")
		switch status {
		case "completed", "failed", "stopped":
		case "killed":
			status = "stopped"
		default:
			continue
		}
		out = append(out, event.TaskDone{ID: tag(seg, "task-id"), CallID: tag(seg, "tool-use-id"),
			Status: status, OutputFile: tag(seg, "output-file"), Summary: tag(seg, "summary")})
	}
	return out
}

func tag(s, name string) string {
	_, rest, ok := strings.Cut(s, "<"+name+">")
	if !ok {
		return ""
	}
	v, _, _ := strings.Cut(rest, "</"+name+">")
	return strings.TrimSpace(v)
}

var (
	_ agent.HistoryReader           = Adapter{}
	_ agent.TailReader              = Adapter{}
	_ agent.HistoryTailBeforeReader = Adapter{}
)
