package host

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// resumeHost is a host with no agent, whose ring the test fills.
func resumeHost(t *testing.T) *server {
	t.Helper()
	t.Setenv("RUSH_HOME", t.TempDir())
	s := &server{cfg: Config{ID: "resume"}, clients: map[*conn]struct{}{}, pending: map[string]asked{}}
	s.info.StartedAt, s.info.State = time.Now(), "idle"
	if err := os.MkdirAll(dir(s.cfg.ID), 0o700); err != nil {
		t.Fatal(err)
	}
	return s
}

func echoLine(text string) []byte {
	b, _ := jsonx.Marshal(map[string]any{"type": "user", "agtop_sent": true, "message": map[string]string{"role": "user", "content": text}})
	return b
}

func msgLine(text string) []byte {
	b, _ := eventLine(event.Message{Role: "assistant", ID: "m-" + text, Parts: []event.Part{{Kind: event.Text, Text: text}}})
	return b
}

// limitLine is a line of Claude Code's that becomes three frames: the
// quota, the billing and the limit.
const limitLine = `{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"five_hour","resetsAt":1790000000,"unifiedWindows":{"five_hour":{"utilization":1,"resetsAt":1790000000}}}}`

// put records lines as the agent's would be.
func put(s *server, lines ...[]byte) {
	for _, l := range lines {
		s.tap(l)
	}
}

// upTo records messages until the ring's last line is n.
func upTo(s *server, n int) {
	for i := 0; ; i++ {
		s.mu.Lock()
		at := s.line
		s.mu.Unlock()
		if at >= n {
			return
		}
		put(s, msgLine(fmt.Sprint("filler ", i)))
	}
}

type viewer struct{ lines chan []byte }

// watch opens a viewer of s that says resume after after.
func watch(t *testing.T, s *server, after string) *viewer {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { b.Close() })
	go s.serve(a)
	hi, _ := jsonx.Marshal(op{Op: "hello", Proto: Proto, Resume: true, After: after})
	go b.Write(append(hi, '\n'))
	v := &viewer{lines: make(chan []byte, 1<<16)}
	go func() {
		defer close(v.lines)
		r := jsonx.NewLineReader(b)
		for {
			l, ok := r.Next()
			if !ok {
				return
			}
			v.lines <- bytes.Clone(l)
		}
	}()
	return v
}

func (v *viewer) next(t *testing.T) []byte {
	t.Helper()
	select {
	case l, ok := <-v.lines:
		if !ok {
			t.Fatal("the viewer was let go")
		}
		return l
	case <-time.After(5 * time.Second):
		t.Fatal("nothing came")
		return nil
	}
}

// catchUp is what comes before synced, and synced's position.
func (v *viewer) catchUp(t *testing.T) ([][]byte, string) {
	t.Helper()
	var got [][]byte
	for {
		l := v.next(t)
		if ev, err := Decode(l); err == nil {
			if s, ok := ev.(Synced); ok {
				return got, s.Position
			}
		}
		got = append(got, l)
	}
}

func isReset(l []byte) bool {
	ev, err := Decode(l)
	_, ok := ev.(Reset)
	return err == nil && ok
}

// places is the numbered frames' places, in order.
func places(lines [][]byte) []string {
	var out []string
	for _, l := range lines {
		if p := Numbered(l); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (s *server) inc() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.incarnation()
}

func TestEveryFrameOnAChatStreamHasANumber(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"), []byte(limitLine), msgLine("ok"))
	got, synced := watch(t, s, "").catchUp(t)
	if len(got) == 0 || !isReset(got[0]) {
		t.Fatalf("no position should start with a reset: %s", got)
	}
	// The time mark, the echo, the three frames of the limit, the message.
	want := []string{"1.0", "2.0", "3.0", "3.1", "3.2", "4.0"}
	if p := places(got); strings.Join(p, " ") != strings.Join(want, " ") {
		t.Fatalf("places %q, want %q", p, want)
	}
	if synced != s.inc()+":4.0" {
		t.Errorf("synced %q", synced)
	}
	// A frame keeps reading as it did: the place is only an extra field.
	if ev, err := Decode(got[len(got)-1]); err != nil {
		t.Fatalf("%s: %v", got[len(got)-1], err)
	} else if _, ok := ev.(InfoEvent); !ok {
		t.Fatalf("the info comes last before synced, got %T", ev)
	}
}

func TestTheRuntimeAnswersAPositionRunsAndHoldsLine120(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"))
	upTo(s, 130)
	v := watch(t, s, s.inc()+":120.0")
	got, synced := v.catchUp(t)
	for _, l := range got {
		if isReset(l) {
			t.Fatal("a position the host holds resumes, with no reset")
		}
	}
	p := places(got)
	if len(p) != 10 || p[0] != "121.0" || p[9] != "130.0" {
		t.Fatalf("places %q", p)
	}
	if synced != s.inc()+":130.0" {
		t.Errorf("synced %q", synced)
	}
	put(s, msgLine("live"))
	if l := v.next(t); Numbered(l) != "131.0" || !bytes.Contains(l, []byte("live")) {
		t.Errorf("live: %s", l)
	}
}

// resets is that a viewer asking after after gets a reset, the tail, then
// synced, then live frames.
func resets(t *testing.T, s *server, after string) {
	t.Helper()
	v := watch(t, s, after)
	got, synced := v.catchUp(t)
	if len(got) == 0 || !isReset(got[0]) {
		t.Fatalf("no reset first: %.200s", got)
	}
	if len(places(got)) == 0 {
		t.Fatal("no tail")
	}
	if !strings.HasPrefix(synced, s.inc()+":") {
		t.Errorf("synced %q", synced)
	}
	put(s, msgLine("live"))
	for {
		if l := v.next(t); bytes.Contains(l, []byte("live")) {
			if Numbered(l) == "" {
				t.Errorf("live frame with no place: %s", l)
			}
			return
		}
	}
}

func TestTheRuntimeAnswersAPositionRunsWithNoPosition(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"))
	upTo(s, 130)
	resets(t, s, "")
}

func TestTheRuntimeAnswersAPositionWasRestarted(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"))
	upTo(s, 130)
	old := s.inc()
	s.info.StartedAt = s.info.StartedAt.Add(time.Second) // the same lines, another run
	resets(t, s, old+":120.0")
}

func TestTheRuntimeAnswersAPositionWasRewound(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"))
	upTo(s, 130)
	before := s.inc()
	s.mu.Lock()
	err := s.rewind("other", true, nil)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	put(s, echoLine("again"))
	upTo(s, 140)
	resets(t, s, before+":120.0")
}

func TestTheRuntimeAnswersAPositionTrimmedLine120(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"))
	upTo(s, 130)
	// Trimmed as trim leaves it: the ring starts after the last line gone.
	s.mu.Lock()
	k := 120
	s.ring, s.seq, s.gone = s.ring[k:], s.seq[k:], s.seq[k-1]
	s.mu.Unlock()
	if s.gone != 120 {
		t.Fatalf("gone %d", s.gone)
	}
	resets(t, s, s.inc()+":120.0")
	// The line after it still resumes.
	if got, _ := watch(t, s, s.inc()+":121.0").catchUp(t); len(got) > 0 && isReset(got[0]) {
		t.Error("a line still held reset")
	}
}

// trim keeps the number of the last line it drops.
func TestTrimKeepsTheLastLineGone(t *testing.T) {
	s := resumeHost(t)
	big := strings.Repeat("x", 1<<20)
	for i := range 12 {
		put(s, echoLine(fmt.Sprint("turn ", i)), msgLine(big))
		time.Sleep(time.Millisecond)
	}
	if s.gone == 0 || s.seq[0] != s.gone+1 || len(s.seq) != len(s.ring) {
		t.Fatalf("gone %d, ring from %d, %d numbers for %d lines", s.gone, s.seq[0], len(s.seq), len(s.ring))
	}
}

func TestTheRuntimeAnswersAPositionHoldsMoreThan1MBAfterLine120(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("hi"))
	upTo(s, 130)
	put(s, msgLine(strings.Repeat("y", 600<<10)), msgLine(strings.Repeat("z", 600<<10)))
	resets(t, s, s.inc()+":120.0")
}

func TestAResumeStartsAtTheFrameAfterThePosition(t *testing.T) {
	s := resumeHost(t)
	put(s, echoLine("list it"),
		[]byte(`{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"ls"}}]}}`),
		[]byte(limitLine),
		[]byte(`{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"a b"}]}}`))
	// 1 the time mark, 2 the echo, 3 the call, 4 the limit's three frames.
	got, _ := watch(t, s, s.inc()+":4.0").catchUp(t)
	p := places(got)
	if len(p) < 3 || p[0] != "4.1" || p[1] != "4.2" || p[2] != "5.0" {
		t.Fatalf("places %q", p)
	}
	for _, l := range got {
		if Numbered(l) != "5.0" {
			continue
		}
		ev, err := Decode(l)
		m, ok := ev.(event.Message)
		if err != nil || !ok || len(m.Parts) == 0 || m.Parts[0].Output == nil {
			t.Fatalf("the result: %s", l)
		}
		// A shell's result has its exit: known only from the call.
		if m.Parts[0].Output.Exit == nil {
			t.Errorf("the result doesn't know its call: %+v", m.Parts[0].Output)
		}
	}
}

func TestNoFrameIsLostOrSentTwiceWhileAStreamOpens(t *testing.T) {
	s := resumeHost(t)
	s.commands = commandsLine([]event.Command{{Name: "review"}})
	if err := appendExchange(s.cfg.ID, event.Exchange{ID: "x1", Direction: "received", Phase: "message", Text: "from a peer"}); err != nil {
		t.Fatal(err)
	}
	put(s, echoLine("go"))
	upTo(s, 20)
	got, at := watch(t, s, "").catchUp(t)
	if !bytes.Contains(bytes.Join(got, nil), []byte("from a peer")) {
		t.Fatal("a reset sends the archived exchanges")
	}

	const writes = 250 // within the tail's bound, or a late viewer resets
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the chat working while the app opens it
		defer wg.Done()
		for i := range writes {
			put(s, msgLine(fmt.Sprint("w", i)))
		}
	}()
	_, asked, _ := parsePosition(at)
	const viewers = 6
	errs := make(chan string, viewers)
	for range viewers {
		v := watch(t, s, at)
		go func() {
			var got []string
			var last position
			for l := range v.lines {
				ev, err := Decode(l)
				if err != nil {
					continue
				}
				switch ev := ev.(type) {
				case Reset:
					errs <- "a held position reset"
					return
				case Synced:
					// The last frame sent, or the one asked after when none.
					_, p, _ := parsePosition(ev.Position)
					if want := last; p != want && (last != position{} || p != asked) {
						errs <- fmt.Sprintf("synced %s after %s", ev.Position, last)
						return
					}
				case event.Exchange:
					errs <- "a resume sent an archived exchange again"
					return
				}
				p := Numbered(l)
				if p == "" {
					continue
				}
				_, at, _ := parsePosition("x:" + p)
				if at.line <= last.line && last.line != 0 {
					errs <- fmt.Sprintf("%s after %s", p, last)
					return
				}
				last = at
				if m, ok := ev.(event.Message); ok && len(m.Parts) > 0 {
					got = append(got, m.Parts[0].Text)
					if m.Parts[0].Text == fmt.Sprint("w", writes-1) {
						break
					}
				}
			}
			// Every write, once, in order: those before the viewer opened
			// in its catch-up, the rest live.
			for i, text := range got {
				if text != fmt.Sprint("w", i) {
					errs <- fmt.Sprintf("frame %d is %q", i, text)
					return
				}
			}
			if len(got) != writes {
				errs <- fmt.Sprintf("%d of %d writes", len(got), writes)
				return
			}
			errs <- ""
		}()
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	for range viewers {
		select {
		case e := <-errs:
			if e != "" {
				t.Error(e)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a viewer never saw the last write")
		}
	}
}

// synced comes after the buffer, the commands and the info.
func TestSyncedComesAfterTheCommandsAndTheInfo(t *testing.T) {
	s := resumeHost(t)
	s.commands = commandsLine([]event.Command{{Name: "review"}})
	put(s, echoLine("hi"))
	for _, after := range []string{"", s.inc() + ":1.0"} {
		got, _ := watch(t, s, after).catchUp(t)
		var kinds []string
		for _, l := range got {
			switch ev, _ := Decode(l); ev.(type) {
			case Commands:
				kinds = append(kinds, "commands")
			case InfoEvent:
				kinds = append(kinds, "info")
			}
		}
		if strings.Join(kinds, " ") != "commands info" || Numbered(got[len(got)-1]) != "" {
			t.Errorf("after %q: %v", after, kinds)
		}
	}
}

// A ring with nothing in it yet syncs at line 0, and resumes from there.
func TestAnEmptyRingSyncsAtZero(t *testing.T) {
	s := resumeHost(t)
	_, synced := watch(t, s, "").catchUp(t)
	if synced != s.inc()+":0.0" {
		t.Fatalf("synced %q", synced)
	}
	put(s, echoLine("hi"))
	got, _ := watch(t, s, synced).catchUp(t)
	if len(got) > 0 && isReset(got[0]) || strings.Join(places(got), " ") != "1.0 2.0" {
		t.Fatalf("from 0.0: %q", places(got))
	}
}

func TestPositionsRead(t *testing.T) {
	for in, want := range map[string]bool{"a-b:12.3": true, "a:b:1.0": true, "": false, "a:1": false, "a:x.0": false, "a:-1.0": false} {
		if _, _, ok := parsePosition(in); ok != want {
			t.Errorf("%q: %v", in, ok)
		}
	}
	if inc := resumeHost(t).incarnation(); strings.Contains(inc, ":") {
		t.Errorf("incarnation %q holds a colon", inc)
	}
	if got := Numbered([]byte(`{"type":"agtop_ev","ev":{},"agtop_s":"12.3"}`)); got != "12.3" {
		t.Errorf("numbered %q", got)
	}
}
