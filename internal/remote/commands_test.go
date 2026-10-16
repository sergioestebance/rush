package remote

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/agent/event"
	"github.com/0xdeafcafe/rush/internal/host"
)

// The web stream passes a host's commands line on as the commands event.
func TestWireLineSendsCommands(t *testing.T) {
	line := []byte(`{"type":"agtop_commands","commands":[{"name":"compact","description":"Keep a summary","argumentHint":"[focus]","aliases":["c"]}]}`)
	b := wireLine(line)
	ev, err := event.Unmarshal(b)
	if err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	want := event.Commands{List: []event.Command{{Name: "compact", Description: "Keep a summary", ArgumentHint: "[focus]", Aliases: []string{"c"}}}}
	if !reflect.DeepEqual(ev, want) {
		t.Fatalf("got %+v", ev)
	}
}

// A session whose host is down still sends the last commands it listed.
func TestStoppedSessionSendsItsCommands(t *testing.T) {
	s := newServer(t)
	cwd := t.TempDir()
	if err := os.WriteFile(filepath.Join(cwd, "sid"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(host.Root(), "c1c1c1c1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	b, _ := jsonx.Marshal(host.Info{ID: "c1c1c1c1", SessionID: "sid", Kind: "count", Cwd: cwd, State: "stopped", UpdatedAt: time.Now()})
	if err := os.WriteFile(filepath.Join(dir, "info.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	list := []event.Command{{Name: "review", ArgumentHint: "[pr]"}}
	if err := host.WriteCommands("c1c1c1c1", list); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/api/sessions/c1c1c1c1/events", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 64))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got []event.Command
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		ev, err := event.Unmarshal([]byte(data))
		if err != nil {
			continue
		}
		switch e := ev.(type) {
		case event.Commands:
			got = e.List
		case event.Message:
			if !reflect.DeepEqual(got, list) {
				t.Fatalf("history came with commands %+v", got)
			}
			return
		}
	}
	t.Fatal("the stream ended")
}
