package host

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// A client says hello first, with its protocol (op "hello", its Proto). One
// from eventsFrom on gets a Claude Code session as rush's own events, as
// any other agent's session comes; one that says less, or sends no hello
// (an older rush), gets Claude Code's lines as it always has. Hosts from
// before the hello take it for an op they don't know, and do nothing.

// eventsFrom is the first client protocol that reads Claude Code's
// sessions as rush's own events.
const eventsFrom = 6

// helloWait is how long a new connection's first line may take to say
// what the client reads, before it's taken for an older rush's.
const helloWait = 300 * time.Millisecond

// ResumeFrom is the first protocol that can ask, in its hello, for a
// catch-up resumed from a position (resume.go): a host's Info.Proto says
// whether it can.
const ResumeFrom = 12

// greeting is what a client's hello says: its protocol, and whether it
// wants its catch-up resumed from after, or else the tail.
type greeting struct {
	proto  int
	resume bool
	after  string
}

// hello reads a connection's first line, waiting up to helloWait for it:
// what the client says, zero without a hello, and what's left to read of
// the connection as ops, the first line with it when that wasn't a hello.
func hello(nc net.Conn) (greeting, io.Reader) {
	r := bufio.NewReaderSize(nc, 64<<10)
	_ = nc.SetReadDeadline(time.Now().Add(helloWait))
	line, err := r.ReadSlice('\n')
	_ = nc.SetReadDeadline(time.Time{})
	if err == nil {
		var o op
		if jsonx.Unmarshal(line, &o) == nil && o.Op == "hello" {
			return greeting{proto: o.Proto, resume: o.Resume && o.Proto >= ResumeFrom, after: o.After}, r
		}
	}
	// No hello: what came is the start of the ops, read again from the top.
	return greeting{}, io.MultiReader(bytes.NewReader(bytes.Clone(line)), r)
}

// encoder turns Claude Code's lines into rush's own events, for one client
// that reads them. Everything else, the host's own lines and other agents'
// events, goes as it is.
type encoder struct {
	n   agent.Neutral
	buf bytes.Buffer
}

// isClaudeLine is a line as Claude Code wrote it, rather than the host,
// told by how it starts: most are, deltas above all.
func isClaudeLine(l []byte) bool {
	return bytes.HasPrefix(l, []byte(`{"type":"`)) && !bytes.HasPrefix(l, []byte(`{"type":"agtop_`))
}

// claudeEvents is a line of Claude Code's as rush's own events, and false
// for the host's own lines. Claude doesn't always start with its type (a
// turn's result may not), so a line that doesn't is taken apart to tell.
func (e *encoder) claudeEvents(l []byte) ([]event.Event, bool) {
	var ev any
	var err error
	if isClaudeLine(l) {
		ev, err = nativeLine(l)
	} else {
		ev, err = Decode(l)
	}
	if err != nil {
		return nil, false
	}
	return neutral(&e.n).Event(ev)
}

// encode writes line to w as this client reads it, each line ended with a
// newline.
func (e *encoder) encode(w io.Writer, line []byte) error {
	_, err := e.frames(w, line, 0, 0)
	return err
}

// frames is encode for the ring's line number n (0 for a line with none):
// each frame it makes is tagged n.<its place among them>, and those before
// skip are made but not written, so the converter still sees them. It is
// how many frames the line made.
func (e *encoder) frames(w io.Writer, line []byte, n, skip int) (int, error) {
	evs, ok := e.claudeEvents(line)
	if !ok {
		// The host's own, or one it can't read.
		if skip > 0 {
			return 1, nil
		}
		return 1, writeTagged(w, line, n, 0)
	}
	k := 0
	for _, out := range evs {
		b, err := eventLine(out)
		if err != nil {
			continue
		}
		if k >= skip {
			if err := writeTagged(w, b, n, k); err != nil {
				return k, err
			}
		}
		k++
	}
	return k, nil
}

// replay writes a ring line, unpacked, as this client reads it.
func (e *encoder) replay(w io.Writer, u *unpacker, l []byte) error {
	_, err := e.replayN(w, u, l, 0, 0)
	return err
}

// replayN is frames for a ring line, unpacked.
func (e *encoder) replayN(w io.Writer, u *unpacker, l []byte, n, skip int) (int, error) {
	e.buf.Reset()
	if err := u.writeTo(&e.buf, l); err != nil {
		return 0, err
	}
	return e.frames(w, e.buf.Bytes(), n, skip)
}

// writeTagged is writeLine with the frame's place, n.part, as the line's
// last field when n isn't 0: every line the host writes is a JSON object.
func writeTagged(w io.Writer, l []byte, n, part int) error {
	if n == 0 || len(l) < 3 || l[len(l)-1] != '}' {
		return writeLine(w, l)
	}
	if _, err := w.Write(l[:len(l)-1]); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, `,"%s":"%d.%d"}`+"\n", tagKey, n, part)
	return err
}

func writeLine(w io.Writer, l []byte) error {
	if _, err := w.Write(l); err != nil {
		return err
	}
	_, err := w.Write([]byte{'\n'})
	return err
}
