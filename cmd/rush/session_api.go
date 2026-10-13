package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/0xdeafcafe/rush/internal/host"
)

// sessionAPI exposes the versioned embedding interface. Private input is stdin,
// and only the runtime itself knows its host sockets. Watch lifetime is its pipe.
func sessionAPI(args []string, stdin io.Reader, stdout io.Writer, events bool) (bool, error) {
	fs := newFlags("api")
	protocol := fs.Int("protocol", 0, "")
	if err := fs.Parse(args); err != nil {
		return true, err
	}
	var request host.APIRequest
	data, err := io.ReadAll(io.LimitReader(stdin, host.APIInputBytes+1))
	if err == nil && len(data) <= host.APIInputBytes {
		err = json.Unmarshal(data, &request)
	} else if err == nil {
		err = errors.New("input too large")
	}
	if err != nil || *protocol != 1 || fs.NArg() != 0 {
		return true, host.WriteAPIResponse(stdout, host.APIResponse{Protocol: 1, RequestID: request.RequestID, Error: host.APIError("invalid_input", "not_applied")})
	}
	if events {
		return true, host.APIEvents(context.Background(), request, stdout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	response := host.APIExecute(ctx, request)
	return true, host.WriteAPIResponse(stdout, response)
}
