package host

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPublicListOwnedPagesOnlyThisInstallationWithoutMutation(t *testing.T) {
	t.Setenv("RUSH_HOME", t.TempDir())
	installation, _ := NewSessionID()
	otherInstallation, _ := NewSessionID()
	conversation, _ := NewSessionID()
	operation, _ := NewSessionID()
	owner := RuntimeOwnership{Version: "1", Installation: installation, Chat: "chat-one", Operation: operation}
	write := func(id, sid string, metadata RuntimeOwnership) {
		t.Helper()
		if err := os.MkdirAll(dir(id), 0700); err != nil {
			t.Fatal(err)
		}
		cfg := Config{PublicAPI: true, ID: id, SessionID: sid, Meta: apiMetadata(metadata)}
		b, _ := json.Marshal(cfg)
		if err := os.WriteFile(filepath.Join(dir(id), "config.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The first page reaches its fixed scan bound before finding an owner.
	for i := 0; i < apiOwnedScanPerPage; i++ {
		id := "a" + string([]byte{byte('0' + i/100), byte('0' + i/10%10), byte('0' + i%10)})
		if err := os.MkdirAll(dir(id), 0700); err != nil {
			t.Fatal(err)
		}
	}
	write("z-owned-1", conversation, owner)
	write("z-owned-2", conversation, owner) // duplicate mapping stays visible
	write("z-other", conversation, RuntimeOwnership{Version: "1", Installation: otherInstallation, Chat: owner.Chat, Operation: operation})
	write("z-conflict", conversation, owner)
	bad := RuntimeState{Identity: RuntimeIdentity{ConversationID: conversation, HostID: "different-host", HostIncarnation: operation, Metadata: owner}}
	b, _ := json.Marshal(bad)
	if err := os.WriteFile(filepath.Join(dir("z-conflict"), "api-state.json"), b, 0600); err != nil {
		t.Fatal(err)
	}

	request := RuntimeListOwnedRequest{Installation: installation, Limit: 1}
	var got []RuntimeOwnedHost
	for pageCount := 0; pageCount < 8; pageCount++ {
		body, _ := json.Marshal(request)
		response := APIExecute(context.Background(), APIRequest{Protocol: 1, RequestID: operation, Method: "list_owned", Params: body})
		if !response.OK {
			t.Fatalf("list_owned failed: %+v", response.Error)
		}
		page := response.Result.(RuntimeOwnedPage)
		if err := page.Validate(request); err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Items...)
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor <= request.Cursor {
			t.Fatalf("cursor did not advance: %q to %q", request.Cursor, page.NextCursor)
		}
		request.Cursor = page.NextCursor
	}
	if len(got) != 3 {
		t.Fatalf("owned hosts = %+v", got)
	}
	ids := []string{got[0].HostID, got[1].HostID, got[2].HostID}
	if !slices.Equal(ids, []string{"z-conflict", "z-owned-1", "z-owned-2"}) || got[0].Status != "conflict" || got[1].Status != "unknown" || got[2].Status != "unknown" {
		t.Fatalf("wrong ownership, order, or status: %+v", got)
	}
	if got[1].ConversationID != got[2].ConversationID || got[0].State != nil {
		t.Fatal("duplicate or conflicting entry hidden")
	}
	for _, id := range []string{"z-owned-1", "z-owned-2", "z-conflict"} {
		if _, err := os.Stat(filepath.Join(dir(id), "api.sock")); !os.IsNotExist(err) {
			t.Fatalf("list started host %s: %v", id, err)
		}
	}
}

func TestPublicListOwnedRejectsUnboundedRequest(t *testing.T) {
	t.Setenv("RUSH_HOME", t.TempDir())
	installation, _ := NewSessionID()
	requestID, _ := NewSessionID()
	for _, request := range []RuntimeListOwnedRequest{
		{Installation: installation, Limit: 0},
		{Installation: installation, Limit: 101},
		{Installation: installation, Limit: 1, Cursor: "bad cursor"},
		{Installation: "bad", Limit: 1},
	} {
		b, _ := json.Marshal(request)
		response := APIExecute(context.Background(), APIRequest{Protocol: 1, RequestID: requestID, Method: "list_owned", Params: b})
		if response.OK || response.Error == nil || response.Error.Code != "invalid_input" {
			t.Fatalf("accepted invalid page request %+v: %+v", request, response)
		}
	}
}
