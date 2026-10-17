package host

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// A viewer that says resume in its hello gets each ring line's frames
// numbered, "<line>.<part>": the line's number in the ring, one higher than
// the line before (time marks too), and the frame's place among the frames
// the viewer's converter made of it, from 0. A position is
// "<incarnation>:<line>.<part>". Its catch-up is the frames after the
// position it says, when this host still has them all and they're few
// enough; else a reset and the ring's tail. Either ends with synced, after
// the commands and the info.

// tagKey is the field a numbered frame's line carries its place in, last.
const tagKey = "agtop_s"

// The tail's bounds: a catch-up that starts afresh sends at most the last
// tailTurns user turns, tailFrames frames and tailBytes bytes of frames,
// and a resume that would send more starts afresh instead.
const (
	tailTurns  = 10
	tailFrames = 300
	tailBytes  = 1 << 20
)

// position is a frame's place: its line's number and its part.
type position struct{ line, part int }

func (p position) String() string { return strconv.Itoa(p.line) + "." + strconv.Itoa(p.part) }

// parsePosition reads "<incarnation>:<line>.<part>".
func parsePosition(s string) (inc string, p position, ok bool) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return "", p, false
	}
	l, r, ok := strings.Cut(s[i+1:], ".")
	if !ok {
		return "", p, false
	}
	var err1, err2 error
	p.line, err1 = strconv.Atoi(l)
	p.part, err2 = strconv.Atoi(r)
	if err1 != nil || err2 != nil || p.line < 0 || p.part < 0 {
		return "", p, false
	}
	return s[:i], p, true
}

// incarnation names this run of the host and of its conversation: a
// restart or a rewind makes every position before it stale. It holds no
// ':'. Called with mu held.
func (s *server) incarnation() string {
	ms := func(t time.Time) string {
		if t.IsZero() {
			return "0"
		}
		return strconv.FormatInt(t.UnixNano(), 36)
	}
	return ms(s.info.StartedAt) + "-" + ms(s.info.RewoundAt)
}

// view is what a viewer catches up on, taken under the lock that adds it.
type view struct {
	ring [][]byte
	seq  []int
	gone int // the last line trimmed
	last int // the last line given a number
	inc  string
	from time.Time // what's before it is in the transcript: see historyFrom
}

// historyFrom is when what the transcript holds and the ring doesn't ends:
// the latest of the host's start, a rewind and the ring's trim, once the
// conversation has a transcript (began). Called with mu held.
func (s *server) historyFrom() time.Time {
	from := s.info.ReplayFrom
	if s.began || s.cfg.Resume {
		for _, t := range []time.Time{s.info.StartedAt, s.info.RewoundAt} {
			if t.After(from) {
				from = t
			}
		}
	}
	return from
}

// catchUp writes what a resuming viewer is owed of the ring through enc:
// the frames after the position after, when they can be resumed, or else
// a reset and the tail. It reports the last numbered frame it sent (the
// position resumed from when it sent none), whether it resumed, and after
// a reset when the tail's oldest turn began, zero when it's all there.
func (v *view) catchUp(w io.Writer, enc *encoder, after string) (position, bool, time.Time, error) {
	// A line trimmed may have lost the parts after the position's; line 0
	// is before any, which an empty ring's synced says.
	if inc, p, ok := parsePosition(after); ok && inc == v.inc && (p.line > v.gone || p.line == 0 && v.gone == 0) && p.line <= v.last {
		var b bytes.Buffer
		if at, ok := v.resume(enc, &b, p); ok {
			_, err := w.Write(b.Bytes())
			return at, true, time.Time{}, err
		}
		*enc = encoder{} // what it read is the tail's to read again
	}
	at, from, err := v.tail(w, enc)
	return at, false, from, err
}

// resume writes the frames after p to b, the lines up to it read but not
// sent so a tool result still finds its call, and false once they outgrow
// the tail's bounds.
func (v *view) resume(enc *encoder, b *bytes.Buffer, p position) (position, bool) {
	at := p
	cw := &counter{w: b}
	var u unpacker
	for i, l := range v.ring {
		n, skip := v.seq[i], 0
		switch {
		case n < p.line:
			skip = math.MaxInt
		case n == p.line:
			skip = p.part + 1
		}
		k, err := enc.replayN(cw, &u, l, n, skip)
		if err != nil {
			return at, false
		}
		if k > skip {
			at = position{n, k - 1}
		}
		if cw.frames > tailFrames || cw.bytes > tailBytes {
			return at, false
		}
	}
	return at, true
}

// tail writes a reset, then the ring's newest tailTurns user turns, bound
// by tailFrames and tailBytes: a bigger turn keeps its newest part. The
// lines before them are read but not sent. It says when what it left out
// ends, zero when it left nothing.
func (v *view) tail(w io.Writer, enc *encoder) (position, time.Time, error) {
	// First what each line makes, read through a converter of its own.
	type cost struct{ frames, bytes int }
	costs := make([]cost, len(v.ring))
	{
		e, cw := &encoder{}, &counter{w: io.Discard}
		var u unpacker
		for i, l := range v.ring {
			f, b := cw.frames, cw.bytes
			_, _ = e.replayN(cw, &u, l, v.seq[i], 0)
			costs[i] = cost{cw.frames - f, cw.bytes - b}
		}
	}
	// A turn starts at its echo, with the time mark before it: whole turns
	// go, newest first, while they fit, so the cursor reaches the rest.
	// Only a newest turn too big on its own goes in part, its newest.
	var begins []int
	for i, l := range v.ring {
		if isEcho(l) {
			b := i
			if i > 0 && isTimeMark(v.ring[i-1]) {
				b = i - 1
			}
			begins = append(begins, b)
		}
	}
	start, turns, frames, size := len(v.ring), 0, 0, 0
	from := start // where the oldest turn sent begins, whole or not
	for k := len(begins) - 1; k >= -1 && turns < tailTurns; k-- {
		lo := 0
		if k >= 0 {
			lo = begins[k]
		}
		f, b := 0, 0
		for _, c := range costs[lo:start] {
			f, b = f+c.frames, b+c.bytes
		}
		if frames+f > tailFrames || size+b > tailBytes {
			if turns == 0 {
				// Its newest lines, always one.
				for start > lo && (start == len(v.ring) || frames+costs[start-1].frames <= tailFrames && size+costs[start-1].bytes <= tailBytes) {
					start--
					frames, size = frames+costs[start].frames, size+costs[start].bytes
				}
				if from = lo; k >= 0 {
					turns = 1
				}
			}
			break
		}
		if lo == start {
			break
		}
		start, from, frames, size = lo, lo, frames+f, size+b
		if k >= 0 {
			turns++
		}
	}
	// The transcript holds what came before the ring: everything before a
	// restart, a rewind or a trim.
	file := start == 0 && !v.from.IsZero()
	r := resetLine{Type: typeReset, Inc: v.inc, Turns: turns, Frames: frames, Bytes: size,
		Older: start > 0 || file, File: file}
	var cursor time.Time
	if file {
		cursor = v.from
	} else if t := v.markBefore(from); !t.IsZero() {
		cursor = t
	} else {
		cursor = v.from
	}
	if !cursor.IsZero() {
		r.Before = cursor.UnixMilli()
	}
	if !r.Older {
		cursor = time.Time{}
	}
	b, _ := jsonx.Marshal(r)
	if err := writeLine(w, b); err != nil {
		return position{}, cursor, err
	}
	var at position
	var u unpacker
	for i, l := range v.ring {
		skip := 0
		if i < start {
			skip = math.MaxInt
		}
		k, err := enc.replayN(w, &u, l, v.seq[i], skip)
		if err != nil {
			return at, cursor, err
		}
		if k > skip {
			at = position{v.seq[i], k - 1}
		}
	}
	return at, cursor, nil
}

// tailExchanges is what of the archived exchanges a catch-up sends: none
// on a resume, which had them; after a reset those from the tail's oldest
// turn on (from; all when zero), the newest within the tail's bounds.
func tailExchanges(es []event.Exchange, resumed bool, from time.Time) []event.Exchange {
	if resumed {
		return nil
	}
	i, size := len(es), 0
	for i > 0 && len(es)-i < tailFrames {
		e := es[i-1]
		b, _ := jsonx.Marshal(e)
		if !from.IsZero() && e.At.Before(from) || size+len(b) > tailBytes {
			break
		}
		i, size = i-1, size+len(b)
	}
	return es[i:]
}

// markBefore is the time of the last time mark at or before line i of the
// ring: older history is what was written before it.
func (v *view) markBefore(i int) time.Time {
	for i = min(i, len(v.ring)-1); i >= 0; i-- {
		if isTimeMark(v.ring[i]) {
			var m struct {
				T int64 `json:"t"`
			}
			if jsonx.Unmarshal(v.ring[i], &m) == nil {
				return time.UnixMilli(m.T)
			}
		}
	}
	return time.Time{}
}

func isTimeMark(l []byte) bool {
	return bytes.HasPrefix(l, []byte(`{"t":`)) && bytes.Contains(l, []byte(`"type":"`+typeTime+`"`))
}

// resetLine is the line that starts a catch-up afresh: see Reset.
type resetLine struct {
	Type   string `json:"type"`
	Inc    string `json:"inc"`
	Turns  int    `json:"turns"`
	Frames int    `json:"frames"`
	Bytes  int    `json:"bytes"`
	Older  bool   `json:"older,omitzero"`
	File   bool   `json:"file,omitzero"`
	Before int64  `json:"before,omitzero"`
}

// syncedLine ends a catch-up: the position of the last numbered frame
// sent, at.
func syncedLine(inc string, at position) []byte {
	b, _ := jsonx.Marshal(map[string]string{"type": typeSynced, "position": inc + ":" + at.String()})
	return b
}

// counter passes writes on, counting the bytes and the lines.
type counter struct {
	w             io.Writer
	frames, bytes int
}

func (c *counter) Write(p []byte) (int, error) {
	c.bytes += len(p)
	c.frames += bytes.Count(p, []byte{'\n'})
	return c.w.Write(p)
}

// Reset says a resuming client's catch-up starts afresh, with the tail of
// the host's ring: Turns user turns in Frames frames of Bytes bytes. Older
// is whether there's more before it, and File whether that's all in the
// transcript, from before Before (the ring holds none of it). Inc is the
// host's incarnation, for positions.
type Reset struct {
	Inc                  string
	Turns, Frames, Bytes int
	Older, File          bool
	Before               time.Time
}

// Synced ends a resuming client's catch-up: Position is the last numbered
// frame's.
type Synced struct{ Position string }

// Numbered is the place a numbered frame's line carries, "<line>.<part>",
// and "" for a line with none.
func Numbered(l []byte) string {
	i := bytes.LastIndex(l, []byte(`,"`+tagKey+`":"`))
	if i < 0 {
		return ""
	}
	rest := l[i+len(tagKey)+5:]
	j := bytes.IndexByte(rest, '"')
	if j < 0 || string(rest[j:]) != `"}` {
		return ""
	}
	return string(rest[:j])
}

// Position is a frame's place as a client says it back: inc's, at s.
func Position(inc, s string) string { return fmt.Sprintf("%s:%s", inc, s) }
