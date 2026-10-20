package remote

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	claudead "github.com/0xdeafcafe/rush/internal/adapters/claude"
	"github.com/0xdeafcafe/rush/internal/adapters/codex"
	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
	"github.com/0xdeafcafe/rush/internal/host"
)

// turnsAgent's transcript is lines of {"at","user","text"}: a turn is a
// user line and the answers after it.
type turnsAgent struct{ countAgent }

func init() { agent.Register(turnsAgent{}) }

func (turnsAgent) Kind() agent.Kind { return "turns" }

func (turnsAgent) History(s agent.Session, before time.Time) ([]event.Event, error) {
	b, err := os.ReadFile(s.Transcript)
	if err != nil {
		return nil, err
	}
	var out []event.Event
	for l := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		var r struct {
			At   int64
			User bool
			Text string
		}
		if jsonx.Unmarshal([]byte(l), &r) != nil || !before.IsZero() && !time.UnixMilli(r.At).Before(before) {
			continue
		}
		role := "assistant"
		if r.User {
			role = "user"
		}
		out = append(out, event.Message{Role: role, Parts: []event.Part{{Kind: event.Text, Text: r.Text}}})
	}
	return out, nil
}

// stoppedTurns is a stopped session of turnsAgent with n turns, each a
// question "q<i>" and its answer "a<i>".
func stoppedTurns(t *testing.T, id string, n int) {
	t.Helper()
	cwd := t.TempDir()
	var b strings.Builder
	at := time.Now().Add(-time.Hour)
	for i := range n {
		fmt.Fprintf(&b, `{"At":%d,"User":true,"Text":"q%d"}`+"\n", at.Add(time.Duration(2*i)*time.Second).UnixMilli(), i)
		fmt.Fprintf(&b, `{"At":%d,"Text":"a%d"}`+"\n", at.Add(time.Duration(2*i+1)*time.Second).UnixMilli(), i)
	}
	if err := os.WriteFile(filepath.Join(cwd, "sid"), []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	writeInfo(t, host.Info{ID: id, SessionID: "sid", Kind: "turns", Cwd: cwd, State: "stopped", UpdatedAt: time.Now()})
}

func writeInfo(t *testing.T, i host.Info) {
	t.Helper()
	dir := filepath.Join(host.Root(), i.ID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	b, _ := jsonx.Marshal(i)
	if err := os.WriteFile(filepath.Join(dir, "info.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// wire is one SSE frame as the app reads it.
type wire struct {
	T string         `json:"t"`
	S string         `json:"s"`
	E jsontext.Value `json:"e"`
}

// open reads path's SSE frames into a channel, until the response ends.
func open(t *testing.T, ts *httptest.Server, path string) <-chan wire {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 64))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan wire, 4096)
	go func() {
		defer resp.Body.Close()
		defer close(out)
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(nil, 4<<20)
		for sc.Scan() {
			if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				var w wire
				if jsonx.Unmarshal([]byte(data), &w) == nil {
					out <- w
				}
			}
		}
	}()
	return out
}

func take(t *testing.T, c <-chan wire) (wire, bool) {
	t.Helper()
	select {
	case w, ok := <-c:
		return w, ok
	case <-time.After(5 * time.Second):
		t.Fatal("no frame came")
		return wire{}, false
	}
}

// until reads frames up to and with one of type tt.
func through(t *testing.T, c <-chan wire, tt string) []wire {
	t.Helper()
	var got []wire
	for {
		w, ok := take(t, c)
		if !ok {
			t.Fatalf("ended before %s: %v", tt, kinds(got))
		}
		got = append(got, w)
		if w.T == tt {
			return got
		}
	}
}

func kinds(ws []wire) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.T)
	}
	return out
}

func texts(ws []wire) []string {
	var out []string
	for _, w := range ws {
		var m struct{ Parts []event.Part }
		if w.T == "message" && jsonx.Unmarshal(w.E, &m) == nil && len(m.Parts) > 0 {
			out = append(out, m.Parts[0].Text)
		}
	}
	return out
}

func field(t *testing.T, w wire, name string) string {
	t.Helper()
	var m map[string]string
	if err := jsonx.Unmarshal(w.E, &m); err != nil {
		t.Fatalf("%s: %v", w.E, err)
	}
	return m[name]
}

func TestTheRuntimeAnswersAPositionIsStopped(t *testing.T) {
	s := newServer(t)
	stoppedTurns(t, "d0d0d0d0", 30)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	got := through(t, open(t, ts, "/api/sessions/d0d0d0d0/events?after="+url.QueryEscape("old-0:120.0")), "synced")
	k := kinds(got)
	if k[0] != "reset" || k[1] != "info" || k[2] != "cut" {
		t.Fatalf("frames %v", k)
	}
	if field(t, got[len(got)-1], "position") != "" {
		t.Errorf("a stopped host's synced has a position: %s", got[len(got)-1].E)
	}
	if tx := texts(got); len(tx) != 20 || tx[0] != "q20" || tx[19] != "a29" {
		t.Errorf("the tail is %v", tx)
	}
	for _, w := range got {
		if w.S != "" {
			t.Errorf("a frame from the transcript has a place: %+v", w)
		}
	}
}

func TestAStoppedHostSendsAChangedInfoAlone(t *testing.T) {
	s := newServer(t)
	stoppedTurns(t, "d1d1d1d1", 12)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	c := open(t, ts, "/api/sessions/d1d1d1d1/events")
	through(t, c, "synced")
	i, err := host.ReadInfo("d1d1d1d1")
	if err != nil {
		t.Fatal(err)
	}
	i.Name, i.UpdatedAt = "renamed", time.Now().Add(time.Second)
	writeInfo(t, i)
	for {
		w, _ := take(t, c)
		if w.T == "info" {
			if !strings.Contains(string(w.E), "renamed") {
				t.Fatalf("info %s", w.E)
			}
			break
		}
		if w.T != "" {
			t.Fatalf("before the info: %s", w.T)
		}
	}
	select {
	case w := <-c:
		t.Fatalf("after the info came %s", w.T)
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestOlderTurnsComeFromTheHistoryRoute(t *testing.T) {
	s := newServer(t)
	stoppedTurns(t, "d2d2d2d2", 30)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	tail := through(t, open(t, ts, "/api/sessions/d2d2d2d2/events"), "synced")
	cur := field(t, tail[2], "before")
	for page, want := range [][2]string{{"q10", "a19"}, {"q0", "a9"}} {
		var got []wire
		for w := range open(t, ts, "/api/sessions/d2d2d2d2/history?before="+url.QueryEscape(cur)) {
			got = append(got, w)
		}
		tx := texts(got)
		if len(tx) != 20 || tx[0] != want[0] || tx[19] != want[1] {
			t.Fatalf("page %d: %v", page, tx)
		}
		last := got[len(got)-1]
		if page == 0 {
			if last.T != "cut" {
				t.Fatalf("page %d ends with %s", page, last.T)
			}
			cur = field(t, last, "before")
		} else if last.T == "cut" {
			t.Fatal("the oldest page has a cut")
		}
	}
	if w := do(t, s.Handler(), "GET", "/api/sessions/d2d2d2d2/history?before=nonsense", "", map[string]string{"Authorization": "Bearer " + strings.Repeat("a", 64)}); w.Code != http.StatusBadRequest {
		t.Errorf("a cursor of nothing: %d", w.Code)
	}
}

// coldTail checks a transcript's tail for its 200 turns, each a question
// "q<i>" and an answer.
func coldTail(t *testing.T, kind agent.Kind, path string) {
	t.Helper()
	before := time.Now()
	frames, next, more := sessionTail(kind, agent.Session{Transcript: path}, before, place{}, tailTurns, tailFrames, tailBytes)
	if !more || next != (place{skip: tailTurns}) {
		t.Fatalf("%+v given, more %v", next, more)
	}
	size := 0
	var ws []wire
	for _, b := range frames {
		size += len(b)
		var w wire
		_ = jsonx.Unmarshal(b, &w)
		ws = append(ws, w)
	}
	if len(frames) > tailFrames || size > tailBytes {
		t.Fatalf("%d frames, %d bytes", len(frames), size)
	}
	var qs []string
	for _, tx := range texts(ws) {
		if strings.HasPrefix(tx, "q") {
			qs = append(qs, tx)
		}
	}
	if len(qs) != 10 || qs[0] != "q190" || qs[9] != "q199" {
		t.Fatalf("questions %v", qs)
	}
}

func TestAColdChatGetsOnlyItsTailClaude(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.jsonl")
	var b strings.Builder
	at := time.Now().Add(-time.Hour)
	stamp := func(i int) string { return at.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	for i := range 200 {
		fmt.Fprintf(&b, `{"type":"user","uuid":"u%d","message":{"role":"user","content":"q%d"},"timestamp":%q}`+"\n", i, i, stamp(2*i))
		fmt.Fprintf(&b, `{"type":"assistant","uuid":"a%d","message":{"id":"m%d","role":"assistant","content":[{"type":"text","text":"a%d %s"}]},"timestamp":%q}`+"\n", i, i, i, strings.Repeat("w", 20<<10), stamp(2*i+1))
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	coldTail(t, claudead.Kind, path)
}

func TestAColdChatGetsOnlyItsTailCodex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout-2026-09-01-tail.jsonl")
	var b strings.Builder
	at := time.Now().Add(-time.Hour)
	fmt.Fprintf(&b, `{"timestamp":%q,"type":"session_meta","payload":{"id":"tail","cwd":"/tmp"}}`+"\n", at.UTC().Format(time.RFC3339))
	for i := range 200 {
		stamp := at.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339)
		fmt.Fprintf(&b, `{"timestamp":%q,"type":"event_msg","payload":{"type":"task_started","turn_id":"%d"}}`+"\n", stamp, i)
		fmt.Fprintf(&b, `{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"q%d"}]}}`+"\n", stamp, i)
		fmt.Fprintf(&b, `{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"a%d %s"}]}}`+"\n", stamp, i, strings.Repeat("w", 20<<10))
		fmt.Fprintf(&b, `{"timestamp":%q,"type":"event_msg","payload":{"type":"task_complete"}}`+"\n", stamp)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	coldTail(t, codex.Kind, path)
}

// A turn bigger than the tail keeps its newest part.
func TestABigTurnKeepsItsNewestPart(t *testing.T) {
	var evs []event.Event
	evs = append(evs, event.Message{Role: "user", Parts: []event.Part{{Kind: event.Text, Text: "go"}}})
	for i := range 500 {
		evs = append(evs, event.Message{Role: "assistant", Parts: []event.Part{{Kind: event.Text, Text: fmt.Sprint(i)}}})
	}
	frames, next, more := pickTurns(evs, false, place{}, tailTurns, tailFrames, tailBytes)
	if len(frames) != tailFrames || next != (place{part: tailFrames}) || !more || !strings.Contains(string(frames[len(frames)-1]), `"499"`) {
		t.Fatalf("%d frames, %+v given, more %v", len(frames), next, more)
	}
}

// resumeHost stands in for a host of this protocol: each connection's
// hello is kept, and it's answered with what script says.
func resumeHost(t *testing.T, id string, script func(n int, after string) []string) <-chan string {
	t.Helper()
	dir := filepath.Join(host.Root(), id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeInfo(t, host.Info{ID: id, SessionID: "sid", HostPID: os.Getpid(), State: "idle", Proto: host.Proto, StartedAt: time.Now(), UpdatedAt: time.Now()})
	ln := listenIn(t, dir)
	t.Cleanup(func() { ln.Close() })
	hellos := make(chan string, 16)
	go func() {
		for n := 0; ; n++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			r := bufio.NewReader(c)
			l, _ := r.ReadString('\n')
			var o struct {
				Resume bool
				After  string
			}
			_ = jsonx.Unmarshal([]byte(l), &o)
			hellos <- o.After
			for _, line := range script(n, o.After) {
				c.Write([]byte(line + "\n"))
			}
			if n == 0 {
				c.Close() // dropped: the stream dials again
			} else {
				t.Cleanup(func() { c.Close() })
			}
		}
	}()
	return hellos
}

func evAt(t *testing.T, text, s string) string {
	b, err := event.Marshal(event.Message{Role: "assistant", Parts: []event.Part{{Kind: event.Text, Text: text}}})
	if err != nil {
		t.Fatal(err)
	}
	return `{"type":"agtop_ev","ev":` + string(b) + `,"agtop_s":"` + s + `"}`
}

// A running host's catch-up goes on as frames with their places, and a
// stream dialling it again asks after the last place it forwarded.
func TestTheRuntimeAnswersAPositionRunsAndResumesAcrossADrop(t *testing.T) {
	s := runningServer(t)
	hellos := resumeHost(t, "d3d3d3d3", func(n int, after string) []string {
		info := `{"info":{"id":"d3d3d3d3","state":"idle"},"type":"agtop_info"}`
		if n == 0 {
			return []string{evAt(t, "one", "121.0"), evAt(t, "two", "121.1"), info, `{"position":"inc:121.1","type":"agtop_synced"}`, evAt(t, "three", "122.0")}
		}
		return []string{evAt(t, "four", "123.0"), info, `{"position":"inc:123.0","type":"agtop_synced"}`}
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	c := open(t, ts, "/api/sessions/d3d3d3d3/events?after="+url.QueryEscape("inc:120.0"))
	got := through(t, c, "synced")
	if k := kinds(got); k[0] != "message" || got[0].S != "121.0" || got[1].S != "121.1" {
		t.Fatalf("frames %v %+v", k, got)
	}
	if field(t, got[len(got)-1], "position") != "inc:121.1" {
		t.Errorf("synced %s", got[len(got)-1].E)
	}
	if h := <-hellos; h != "inc:120.0" {
		t.Errorf("first hello after %q", h)
	}
	got = through(t, c, "synced")
	if h := <-hellos; h != "inc:122.0" {
		t.Errorf("the stream dialled again after %q, not its last frame", h)
	}
	for _, w := range got {
		if w.T == "reset" {
			t.Error("a resumed host's stream reset")
		}
	}
}

// A host's reset is forwarded, then cut, then its tail.
func TestTheRuntimeAnswersAPositionRunsWithNoPositionThroughTheStream(t *testing.T) {
	s := runningServer(t)
	resumeHost(t, "d4d4d4d4", func(n int, after string) []string {
		if n > 0 {
			return nil
		}
		return []string{`{"before":1760000000000,"bytes":100,"frames":1,"inc":"inc","older":true,"turns":10,"type":"agtop_reset"}`,
			evAt(t, "one", "5.0"), `{"info":{"id":"d4d4d4d4"},"type":"agtop_info"}`, `{"position":"inc:5.0","type":"agtop_synced"}`}
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	got := through(t, open(t, ts, "/api/sessions/d4d4d4d4/events"), "synced")
	if k := strings.Join(kinds(got), " "); k != "reset cut message info synced" {
		t.Fatalf("frames %s", k)
	}
	if field(t, got[1], "before") != "1760000000000.0" || got[2].S != "5.0" {
		t.Errorf("cut %s, message at %q", got[1].E, got[2].S)
	}
}

// runningServer is newServer in a home short enough for a host's socket.
func runningServer(t *testing.T) *Server {
	t.Helper()
	s := newServer(t)
	shortHome(t)
	if err := UpdateConfig(func(c *Config) { c.Web = true }); err != nil {
		t.Fatal(err)
	}
	return s
}

// A host from before resuming replays all it has, then its info: synced
// follows that, with no position.
func TestTheRuntimeAnswersAPositionRunsOnAnOldHost(t *testing.T) {
	s := runningServer(t)
	f := startFakeHost(t, host.Root(), "e0e0e0e0", "old", "/work", evLine(t, event.Message{Role: "assistant", Parts: []event.Part{{Kind: event.Text, Text: "replayed"}}}))
	old := f.info // a copy: the fake reads its own as it answers
	old.Proto = host.ResumeFrom - 1
	writeInfo(t, old)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	got := through(t, open(t, ts, "/api/sessions/e0e0e0e0/events?after="+url.QueryEscape("x:1.0")), "synced")
	if k := strings.Join(kinds(got), " "); k != "reset message info synced" {
		t.Fatalf("frames %s", k)
	}
	if field(t, got[3], "position") != "" {
		t.Errorf("an old host's synced has a position: %s", got[3].E)
	}
}

// Pages of turns too big for one page each send whole turns, and the next
// cursor goes on from the oldest sent: every turn comes once.
func TestOlderTurnsComeFromTheHistoryRouteInWholeTurns(t *testing.T) {
	var evs []event.Event
	for i := range 30 {
		evs = append(evs, event.Message{Role: "user", Parts: []event.Part{{Kind: event.Text, Text: fmt.Sprint("q", i)}}})
		for j := range 39 {
			evs = append(evs, event.Message{Role: "assistant", Parts: []event.Part{{Kind: event.Text, Text: fmt.Sprint(i, ".", j)}}})
		}
	}
	var seen []string
	skip := 0
	for {
		frames, next, more := pickTurns(evs, false, place{skip: skip}, tailTurns, tailFrames, tailBytes)
		n := next.skip - skip
		var ws []wire
		for _, b := range frames {
			var w wire
			_ = jsonx.Unmarshal(b, &w)
			ws = append(ws, w)
		}
		var qs []string
		for _, tx := range texts(ws) {
			if strings.HasPrefix(tx, "q") {
				qs = append(qs, tx)
			}
		}
		if len(qs) != n || len(frames) != 40*n {
			t.Fatalf("at %d: %d turns said, %d sent in %d frames", skip, n, len(qs), len(frames))
		}
		seen = append(qs, seen...)
		skip += n
		if !more {
			break
		}
	}
	for i, q := range seen {
		if q != fmt.Sprint("q", i) {
			t.Fatalf("turns %v", seen)
		}
	}
	if len(seen) != 30 {
		t.Fatalf("%d of 30 turns", len(seen))
	}
}

// deepAgent's transcript is 200 turns, of which a tail of most bytes holds
// one per MB.
type deepAgent struct{ countAgent }

func init() { agent.Register(deepAgent{}) }

func (deepAgent) Kind() agent.Kind { return "deep" }

func (deepAgent) HistoryTailBefore(_ agent.Session, most int64, _ time.Time) ([]event.Event, bool, error) {
	n := min(200, int(most>>20))
	var evs []event.Event
	for i := 200 - n; i < 200; i++ {
		evs = append(evs, event.Message{Role: "user", Parts: []event.Part{{Kind: event.Text, Text: fmt.Sprint("q", i)}}})
	}
	return evs, n < 200, nil
}

// A page deeper than any window yet read keeps reading: it never says it
// reached the start when it didn't.
func TestOlderTurnsComeFromTheHistoryRouteHoweverDeep(t *testing.T) {
	frames, next, more := sessionTail("deep", agent.Session{}, time.Now(), place{skip: 180}, tailTurns, tailFrames, tailBytes)
	if next.skip != 190 || !more || !strings.Contains(string(frames[0]), `"q10"`) {
		t.Fatalf("%+v given, more %v", next, more)
	}
	if _, next, more = sessionTail("deep", agent.Session{}, time.Now(), place{skip: 190}, tailTurns, tailFrames, tailBytes); next.skip != 200 || more {
		t.Fatalf("the oldest page: %+v given, more %v", next, more)
	}
}

// A host that stops before its catch-up ends still gets the client to
// synced: the stopped host's.
func TestAStoppedHostSendsAChangedInfoAloneOnceSynced(t *testing.T) {
	s := runningServer(t)
	id := "e1e1e1e1"
	resumeHost(t, id, func(n int, after string) []string {
		if n == 0 { // it goes before its synced
			i, _ := host.ReadInfo(id)
			i.HostPID, i.State, i.UpdatedAt = 0, "stopped", time.Now()
			writeInfo(t, i)
			return []string{evAt(t, "one", "121.0")}
		}
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	got := through(t, open(t, ts, "/api/sessions/"+id+"/events?after="+url.QueryEscape("inc:120.0")), "synced")
	if k := strings.Join(kinds(got), " "); !strings.HasPrefix(k, "message reset info") {
		t.Fatalf("frames %s", k)
	}
	if field(t, got[len(got)-1], "position") != "" {
		t.Errorf("synced %s", got[len(got)-1].E)
	}
}

// A host that sent a big turn in part says a cursor inside it: the
// history route gives back the rest of it, its prompt last, each once,
// though that rest is bigger than a page.
func TestABigTurnsStartComesFromTheHistoryRoute(t *testing.T) {
	for _, small := range []bool{false, true} {
		s := runningServer(t)
		id := "d6d6d6d6"
		if small {
			id = "d7d7d7d7"
		}
		at := time.Now().Add(-time.Hour)
		cut := at.Add(time.Second + 350*time.Millisecond) // the host sent answer 350 on
		resumeHost(t, id, func(n int, after string) []string {
			if n > 0 {
				return nil
			}
			return []string{fmt.Sprintf(`{"before":%d,"bytes":100,"frames":70,"inc":"inc","older":true,"turns":1,"type":"agtop_reset"}`, cut.UnixMilli()),
				evAt(t, "answer 350", "5.0"), `{"position":"inc:5.0","type":"agtop_synced"}`}
		})
		cwd := t.TempDir()
		var b strings.Builder
		var want []string
		if small {
			fmt.Fprintf(&b, `{"At":%d,"User":true,"Text":"hi"}`+"\n"+`{"At":%d,"Text":"hello"}`+"\n", at.UnixMilli(), at.UnixMilli()+1)
			want = append(want, "hi", "hello")
		}
		fmt.Fprintf(&b, `{"At":%d,"User":true,"Text":"go"}`+"\n", at.Add(time.Second-time.Millisecond).UnixMilli())
		want = append(want, "go")
		for i := range 420 {
			fmt.Fprintf(&b, `{"At":%d,"Text":"answer %d"}`+"\n", at.Add(time.Second+time.Duration(i)*time.Millisecond).UnixMilli(), i)
			if i < 350 {
				want = append(want, fmt.Sprint("answer ", i))
			}
		}
		if err := os.WriteFile(filepath.Join(cwd, "sid"), []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		i, _ := host.ReadInfo(id)
		i.Kind, i.Cwd = "turns", cwd
		writeInfo(t, i)
		ts := httptest.NewServer(s.Handler())
		t.Cleanup(ts.Close)
		tail := through(t, open(t, ts, "/api/sessions/"+id+"/events"), "synced")
		if k := strings.Join(kinds(tail), " "); k != "reset cut message synced" {
			t.Fatalf("small %v: frames %s", small, k)
		}
		var got []string
		cur := field(t, tail[1], "before")
		for range 5 {
			var page []wire
			for w := range open(t, ts, "/api/sessions/"+id+"/history?before="+url.QueryEscape(cur)) {
				page = append(page, w)
			}
			got = append(texts(page), got...)
			if len(page) > 0 && page[len(page)-1].T == "cut" {
				cur = field(t, page[len(page)-1], "before")
				continue
			}
			cur = ""
			break
		}
		if cur != "" || strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("small %v: %d of %d came, %.3q", small, len(got), len(want), got)
		}
	}
}

// A big turn's pages go on from the oldest event sent, with its start
// last.
func TestABigTurnsPagesReachItsStart(t *testing.T) {
	var evs []event.Event
	evs = append(evs, event.Message{Role: "user", Parts: []event.Part{{Kind: event.Text, Text: "go"}}})
	for i := range 700 {
		evs = append(evs, event.Message{Role: "assistant", Parts: []event.Part{{Kind: event.Text, Text: fmt.Sprint(i)}}})
	}
	var at place
	var all int
	for page := 0; ; page++ {
		frames, next, more := pickTurns(evs, false, at, tailTurns, tailFrames, tailBytes)
		all += len(frames)
		if !more {
			if page != 2 || all != len(evs) || !strings.Contains(string(frames[0]), `"go"`) || next.skip != 1 {
				t.Fatalf("page %d: %d of %d, %+v given", page, all, len(evs), next)
			}
			return
		}
		if next.part != all {
			t.Fatalf("page %d: %+v given, %d sent", page, next, all)
		}
		at = next
	}
}
