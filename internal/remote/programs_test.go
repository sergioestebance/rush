package remote

import (
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/0xdeafcafe/rush/internal/host"
)

// A start's program and environment reach the host only from a serve told
// to take them, and the program only from one of its folders.
func TestStartProgramsOnlyFromServeFolders(t *testing.T) {
	s := newServer(t)
	var started *StartRequest
	s.Start = func(r StartRequest) (host.Info, error) { started = &r; return host.Info{ID: "abcd1234"}, nil }
	hdr := map[string]string{"Authorization": "Bearer " + s.Token, "Content-Type": "application/json"}
	work, run, other := t.TempDir(), t.TempDir(), t.TempDir()
	prog := filepath.Join(run, "agent")
	stray := filepath.Join(other, "agent")
	plain := filepath.Join(run, "notes")
	for p, mode := range map[string]os.FileMode{prog: 0o755, stray: 0o755, plain: 0o644} {
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"), mode); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(run, "out")
	if err := os.Symlink(stray, link); err != nil {
		t.Fatal(err)
	}
	start := func(body string) int {
		started = nil
		return do(t, s.Handler(), "POST", "/api/sessions", body, hdr).Code
	}
	q := func(s string) string { return `"` + s + `"` }

	// a serve without folders takes neither
	for _, body := range []string{
		`{"cwd":` + q(work) + `,"binary":` + q(prog) + `}`,
		`{"cwd":` + q(work) + `,"env":{"HTTPS_PROXY":"http://127.0.0.1:9"}}`,
	} {
		if c := start(body); c != http.StatusForbidden || started != nil {
			t.Fatalf("no folders, %s: %d, started %v", body, c, started)
		}
	}
	// without either, a start is as before
	if c := start(`{"cwd":` + q(work) + `}`); c != 200 || started == nil || started.Binary != "" || started.Env != nil {
		t.Fatalf("plain start: %d %+v", c, started)
	}
	// a start names its own mode, in place of rush's settings
	if c := start(`{"cwd":` + q(work) + `,"mode":"bypassPermissions"}`); c != 200 || started == nil || started.Mode != "bypassPermissions" {
		t.Fatalf("mode start: %d %+v", c, started)
	}

	s.Programs = []string{run}
	if c := start(`{"cwd":` + q(work) + `,"binary":` + q(prog) + `,"env":{"B":"2","A":"1=x"}}`); c != 200 || started == nil {
		t.Fatalf("program start: %d", c)
	}
	if started.Binary != prog || !slices.Equal(started.EnvList(), []string{"A=1=x", "B=2"}) {
		t.Fatalf("host got %q %v", started.Binary, started.EnvList())
	}
	for name, body := range map[string]string{
		"another folder":    `{"cwd":` + q(work) + `,"binary":` + q(stray) + `}`,
		"a link out":        `{"cwd":` + q(work) + `,"binary":` + q(link) + `}`,
		"not runnable":      `{"cwd":` + q(work) + `,"binary":` + q(plain) + `}`,
		"missing":           `{"cwd":` + q(work) + `,"binary":` + q(filepath.Join(run, "gone")) + `}`,
		"relative":          `{"cwd":` + q(work) + `,"binary":"agent"}`,
		"the folder itself": `{"cwd":` + q(work) + `,"binary":` + q(run) + `}`,
		"rush's own":        `{"cwd":` + q(work) + `,"env":{"RUSH_SESSION":"x"}}`,
		"a bad name":        `{"cwd":` + q(work) + `,"env":{"A=B":"x"}}`,
	} {
		if c := start(body); c != http.StatusForbidden || started != nil {
			t.Errorf("%s: %d, started %v", name, c, started)
		}
	}
}

// A fork names the conversation it copies, and resumes none.
func TestStartForkFrom(t *testing.T) {
	s := newServer(t)
	var started *StartRequest
	s.Start = func(r StartRequest) (host.Info, error) { started = &r; return host.Info{ID: "abcd1234"}, nil }
	hdr := map[string]string{"Authorization": "Bearer " + s.Token, "Content-Type": "application/json"}
	work := t.TempDir()
	src := "0f9b1c2d-3e4f-4a5b-8c6d-7e8f9a0b1c2d"
	start := func(body string) int {
		started = nil
		return do(t, s.Handler(), "POST", "/api/sessions", body, hdr).Code
	}
	if c := start(`{"cwd":"` + work + `","forkFrom":"` + src + `"}`); c != 200 || started == nil || started.ForkFrom != src {
		t.Fatalf("fork start: %d %+v", c, started)
	}
	for _, body := range []string{
		`{"cwd":"` + work + `","forkFrom":"not-a-conversation"}`,
		`{"cwd":"` + work + `","forkFrom":"` + src + `","resume":"` + src + `"}`,
	} {
		if c := start(body); c != http.StatusBadRequest || started != nil {
			t.Fatalf("%s: %d, started %v", body, c, started)
		}
	}
}
