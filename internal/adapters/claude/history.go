package claude

import (
	"encoding/json/jsontext"
	"errors"
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

var _ agent.HistoryReader = Adapter{}
