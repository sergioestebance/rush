package headless

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"
)

func TestExplicitForkArgumentsKeepSourceAndTargetSeparate(t *testing.T) {
	options := Options{ForkFrom: "11111111-1111-4111-8111-111111111111", SessionID: "22222222-2222-4222-8222-222222222222"}
	args := options.args()
	for _, pair := range [][2]string{{"--resume", options.ForkFrom}, {"--session-id", options.SessionID}} {
		i := slices.Index(args, pair[0])
		if i < 0 || i+1 >= len(args) || args[i+1] != pair[1] {
			t.Fatalf("missing fork identity %s: %v", pair[0], args)
		}
	}
	if !slices.Contains(args, "--fork-session") {
		t.Fatal("resume plus session-id is invalid without fork-session")
	}
}

func TestConfirmedWriteWaitsForProviderPipe(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	s := &Session{stdin: w, wsig: make(chan struct{}, 1), done: make(chan struct{})}
	go s.writer()
	defer close(s.done)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.WriteConfirmed(ctx, map[string]string{"private": "synthetic"}) }()
	select {
	case <-result:
		t.Fatal("queue acceptance falsely confirmed a pipe write")
	case <-time.After(20 * time.Millisecond):
	}
	b := make([]byte, 128)
	if _, err := r.Read(b); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

type refusedPipe struct{}

func (refusedPipe) Write([]byte) (int, error) { return 0, errors.New("synthetic pipe failure") }
func (refusedPipe) Close() error              { return nil }

func TestConfirmedWriteDoesNotAcknowledgeBrokenPipe(t *testing.T) {
	s := &Session{stdin: refusedPipe{}, wsig: make(chan struct{}, 1), done: make(chan struct{})}
	go s.writer()
	defer close(s.done)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.WriteConfirmed(ctx, map[string]string{"text": "synthetic"}) == nil {
		t.Fatal("broken provider pipe confirmed")
	}
}
