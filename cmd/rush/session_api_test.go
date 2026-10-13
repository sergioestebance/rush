package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestPublicSessionAPIProbeAndInvalidEnvelope(t *testing.T) {
	t.Setenv("RUSH_HOME", t.TempDir())
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{`{"protocol":1,"requestId":"11111111-1111-4111-8111-111111111111","method":"probe","params":{}}`, true},
		{`{"protocol":2,"requestId":"11111111-1111-4111-8111-111111111111","method":"probe","params":{}}`, false},
		{`{"protocol":1,"requestId":"wrong","method":"probe","params":{}}`, false},
		{`{"protocol":1,"requestId":"11111111-1111-4111-8111-111111111111","method":"probe","params":{}} {}`, false},
	} {
		var out bytes.Buffer
		_, err := sessionAPI([]string{"--protocol", "1"}, strings.NewReader(tc.body), &out, false)
		var response struct {
			Protocol int             `json:"protocol"`
			OK       bool            `json:"ok"`
			Result   json.RawMessage `json:"result"`
			Error    json.RawMessage `json:"error"`
		}
		if json.Unmarshal(out.Bytes(), &response) != nil {
			t.Fatalf("no bounded envelope: %q, %v", out.String(), err)
		}
		if response.OK != tc.ok || tc.ok && (response.Protocol != 1 || len(response.Result) == 0) || !tc.ok && len(response.Error) == 0 {
			t.Fatalf("wrong envelope: %s", out.String())
		}
	}
}
