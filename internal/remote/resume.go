package remote

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
	"github.com/0xdeafcafe/rush/internal/convo"
	"github.com/0xdeafcafe/rush/internal/host"
)

// A chat stream resumes: ?after=<position> asks for the frames after it,
// which a running host sends when it still has them, then synced. Without
// one, or when the host can't, it's reset, the tail (the newest tailTurns
// user turns, at most tailFrames frames and tailBytes bytes, across the
// transcript and the host's ring), then synced. When older turns exist,
// cut comes first, with a cursor the history route takes back.

const (
	tailTurns  = 10
	tailFrames = 300
	tailBytes  = 1 << 20
)

// at is where a resuming stream is: the last position it forwarded, to
// dial a host again from within the same stream.
type at struct {
	inc, pos string
	fresh    bool // in a reset's catch-up: until synced, there's no position
	ended    bool // the last host dialled got to synced
}

func newAt(after string) *at {
	a := &at{pos: after}
	if i := strings.LastIndexByte(after, ':'); i >= 0 {
		a.inc = after[:i]
	}
	return a
}

// after is the position to dial a host with.
func (a *at) after() string {
	if a.fresh {
		return ""
	}
	return a.pos
}

// synced is whether the client holds a position its frames came to, its
// last catch-up ended.
func (a *at) synced() bool { return a.ended && !a.fresh && a.pos != "" }

// pumpAfter is pump for a host dialled with DialAfter: its reset is
// followed by the tail's turns from the transcript and its cut, each
// numbered frame carries s, and a tracks the position.
func pumpAfter(ctx interface{ Done() <-chan struct{} }, c *host.Client, w io.Writer, fl http.Flusher, info host.Info, a *at) error {
	keep := time.NewTicker(15 * time.Second)
	defer keep.Stop()
	emit := func(b []byte) error { return sse(w, fl, b) }
	a.ended = false
	for {
		select {
		case <-ctx.Done():
			return errors.New("gone")
		case <-keep.C:
			if err := sse(w, fl, nil); err != nil {
				return err
			}
		case l, ok := <-c.Lines:
			if !ok {
				return nil
			}
			v, err := host.Decode(l)
			if err != nil {
				continue
			}
			switch v := v.(type) {
			case host.Reset:
				a.inc, a.pos, a.fresh = v.Inc, "", true
				if err := emit([]byte(`{"t":"reset","e":null}`)); err != nil {
					return err
				}
				if err := topUp(info, v, emit); err != nil {
					return err
				}
				continue
			case host.Synced:
				a.pos, a.fresh, a.ended = v.Position, false, true
				if i := strings.LastIndexByte(v.Position, ':'); i >= 0 {
					a.inc = v.Position[:i]
				}
				if err := emit(syncedFrame(v.Position)); err != nil {
					return err
				}
				continue
			}
			b := wireLine(l)
			if b == nil {
				continue
			}
			if s := host.Numbered(l); s != "" {
				b = append(b[:len(b)-1], `,"s":"`+s+`"}`...)
				if !a.fresh && a.inc != "" {
					a.pos = a.inc + ":" + s
				}
			}
			if err := emit(b); err != nil {
				return err
			}
		}
	}
}

// topUp follows a host's reset: the cut, and when the ring holds fewer than
// tailTurns turns and the transcript has those before it, the newest of
// them, within what's left of the tail's bounds.
func topUp(info host.Info, r host.Reset, emit func([]byte) error) error {
	if !r.Older {
		return nil
	}
	before := r.Before
	if before.IsZero() {
		before = time.Now()
	}
	if !r.File || r.Turns >= tailTurns || r.Frames >= tailFrames || r.Bytes >= tailBytes {
		return emit(cutFrame(before, 0))
	}
	frames, n, more := fileTail(info, before, 0, tailTurns-r.Turns, tailFrames-r.Frames, tailBytes-r.Bytes)
	if more {
		if err := emit(cutFrame(before, n)); err != nil {
			return err
		}
	}
	for _, b := range frames {
		if err := emit(b); err != nil {
			return err
		}
	}
	return nil
}

// sendFileTail is the tail of what a session's transcript holds from
// before before (all of it when zero), with the cut first: a session whose
// host isn't running, or one before resuming that trimmed its replay.
func sendFileTail(info host.Info, before time.Time, emit func([]byte)) {
	if before.IsZero() {
		before = time.Now()
	}
	frames, n, more := fileTail(info, before, 0, tailTurns, tailFrames, tailBytes)
	if more {
		emit(cutFrame(before, n))
	}
	for _, b := range frames {
		emit(b)
	}
}

// frame is one of the stream's own frames, written as the app reads them:
// t, then e.
type frame struct {
	T string            `json:"t"`
	E map[string]string `json:"e"`
}

func syncedFrame(position string) []byte {
	b, _ := jsonx.Marshal(frame{"synced", map[string]string{"position": position}})
	return b
}

// A cursor is the turns of a transcript written before a time, and how
// many of the newest of them the client has: "<unix ms>.<turns>". The
// transcript only grows after the time, so the turns before it hold still.
func cursor(before time.Time, skip int) string {
	return strconv.FormatInt(before.UnixMilli(), 10) + "." + strconv.Itoa(skip)
}

func parseCursor(s string) (time.Time, int, bool) {
	t, k, ok := strings.Cut(s, ".")
	ms, err1 := strconv.ParseInt(t, 10, 64)
	n, err2 := strconv.Atoi(k)
	if !ok || err1 != nil || err2 != nil || ms <= 0 || n < 0 {
		return time.Time{}, 0, false
	}
	return time.UnixMilli(ms), n, true
}

func cutFrame(before time.Time, skip int) []byte {
	b, _ := jsonx.Marshal(frame{"cut", map[string]string{"before": cursor(before, skip)}})
	return b
}

// history answers GET /api/sessions/{id}/history?before=<cursor>: the
// tailTurns turns before the cursor, oldest first, then a cut with the
// next cursor when there are more. The response ends; it's not a stream.
func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		fail(w, http.StatusNotFound, "no such session")
		return
	}
	before, skip, ok := parseCursor(r.URL.Query().Get("before"))
	if !ok {
		fail(w, http.StatusBadRequest, "no such place in the history")
		return
	}
	info, err := host.ReadInfo(id)
	if err != nil {
		fail(w, http.StatusNotFound, err.Error())
		return
	}
	fl, _ := w.(http.Flusher)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	frames, n, more := fileTail(info, before, skip, tailTurns, tailFrames, tailBytes)
	for _, b := range frames {
		if sse(w, fl, b) != nil {
			return
		}
	}
	if more {
		_ = sse(w, fl, cutFrame(before, skip+n))
	}
}

// fileTail is the frames of the turns of info's transcript written before
// before, leaving out its newest skip turns: the newest turns of them,
// within frames frames and size bytes, a bigger turn keeping its newest
// part. It says how many turns it gave, and whether there are more.
// It reads the transcript from its end where the agent can.
func fileTail(info host.Info, before time.Time, skip, turns, frames, size int) (out [][]byte, n int, more bool) {
	kind := agent.Migrated(info.Kind)
	p := agent.Profile{Kind: kind}
	if cfg, err := host.ReadConfig(info.ID); err == nil {
		p.Dir = cfg.Account.Dir
	}
	s := agent.Session{ID: info.SessionID, Name: info.Name, Cwd: info.Cwd, Profile: p}
	s.Transcript = agent.TranscriptPath(kind, p, info.Cwd, info.SessionID)
	return sessionTail(kind, s, before, skip, turns, frames, size)
}

// sessionTail is fileTail of s, a session of kind.
func sessionTail(kind agent.Kind, s agent.Session, before time.Time, skip, turns, frames, size int) ([][]byte, int, bool) { //nolint:gocritic // as History
	evs, cut := readTurns(kind, s, before, skip+turns)
	return pickTurns(evs, cut, skip, turns, frames, size)
}

// readTurns reads the end of s's transcript before before, at least want
// user turns of it and the start of the one before, where there are that
// many: a tail reader's window grows until it holds them, however deep,
// so a page never says it reached the start when it didn't. cut is whether
// there's more before what it read.
func readTurns(kind agent.Kind, s agent.Session, before time.Time, want int) ([]event.Event, bool) {
	if tb, ok := agent.As[agent.HistoryTailBeforeReader](kind); ok {
		for most := int64(1 << 20); ; most *= 4 {
			evs, cut, err := tb.HistoryTailBefore(s, most, before)
			if err != nil {
				return nil, false
			}
			if !cut || userTurns(evs) > want {
				return evs, cut
			}
		}
	}
	if hr, ok := agent.As[agent.HistoryReader](kind); ok {
		evs, err := hr.History(s, before) // ponytail: reads the whole transcript to keep its end
		if err == nil {
			return evs, false
		}
	}
	return nil, false
}

// isUserTurn is a message that starts a turn: one you said, not one the
// agent put in as yours. Claude's transcript reader marks every user text
// injected (the host tells what you sent, live), so there it's what
// Claude Code's own wrappers aren't.
func isUserTurn(ev event.Event) bool {
	m, ok := ev.(event.Message)
	if !ok || m.Role != "user" || m.Parent != "" {
		return false
	}
	for _, p := range m.Parts {
		if p.Kind != event.Text || strings.TrimSpace(p.Text) == "" {
			continue
		}
		if _, _, wrapped := convo.Injected(p.Text); !m.Injected || !wrapped {
			return true
		}
	}
	return false
}

func userTurns(evs []event.Event) int {
	n := 0
	for _, ev := range evs {
		if isUserTurn(ev) {
			n++
		}
	}
	return n
}

// pickTurns is fileTail's choice from evs, a transcript's end (cut: not
// all of it). Turns go whole, newest first, while they fit: n counts only
// those sent, so the next cursor reaches the rest. Only a newest turn too
// big on its own is sent in part, its newest; its start is lost.
func pickTurns(evs []event.Event, cut bool, skip, turns, frames, size int) (out [][]byte, n int, more bool) {
	var starts []int
	for i, ev := range evs {
		if isUserTurn(ev) {
			starts = append(starts, i)
		}
	}
	if skip > len(starts) {
		return nil, 0, cut
	}
	end, bytes := len(evs), 0
	if skip > 0 {
		end = starts[len(starts)-skip]
	}
	// Turn k is evs[starts[k]:end]; k -1 is what comes before the first,
	// part of a turn whose start wasn't read when cut.
	for k := len(starts) - 1 - skip; k >= -1; k-- {
		if n == turns {
			return out, n, k >= 0 || cut || end > 0
		}
		lo := 0
		if k >= 0 {
			lo = starts[k]
		} else if cut {
			return out, n, true
		}
		var seg [][]byte
		sb := 0
		for _, ev := range evs[lo:end] {
			if b, err := event.Marshal(ev); err == nil {
				seg, sb = append(seg, b), sb+len(b)
			}
		}
		if len(out)+len(seg) > frames || bytes+sb > size {
			if len(out) == 0 {
				// One turn bigger than the tail: its newest part.
				i := len(seg) - 1
				for sb = len(seg[i]); i > 0 && len(seg)-i < frames && sb+len(seg[i-1]) <= size; i-- {
					sb += len(seg[i-1])
				}
				// It counts as given, even before any turn: asked again,
				// it would come the same.
				return seg[i:], 1, true
			}
			return out, n, true
		}
		out, bytes, end = append(seg, out...), bytes+sb, lo
		if k >= 0 {
			n++
		}
	}
	return out, n, false
}
