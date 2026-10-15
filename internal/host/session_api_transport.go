package host

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
)

const APIInputBytes = 8 << 20
const APIOutputBytes = 8 << 20

type APIRequest struct {
	Protocol  int             `json:"protocol"`
	RequestID string          `json:"requestId"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
}

type APIResponse struct {
	Protocol  int           `json:"protocol"`
	RequestID string        `json:"requestId"`
	OK        bool          `json:"ok"`
	Result    any           `json:"result,omitempty"`
	Error     *RuntimeError `json:"error,omitempty"`
}

func APIError(code, outcome string) *RuntimeError {
	message := map[string]string{
		"invalid_input": "Runtime request is invalid.", "invalid_output": "Runtime output cannot be verified.",
		"unsupported": "Required runtime or provider capability is unsupported.", "missing": "Conversation host was not found.",
		"busy": "Another turn or operation is active.", "stale": "Runtime identity or incarnation changed.",
		"conflict": "Conversation ownership is conflicting.", "unknown": "Operation outcome is unconfirmed.",
		"permission": "Runtime storage is inaccessible.", "unavailable": "Runtime host is unavailable.",
		"cancelled": "Runtime observation was cancelled.", "overflow": "Runtime output exceeded its limit.",
		"timeout": "Runtime agent did not become ready in time.", "exited": "Runtime agent exited before it was ready.",
		"folder_missing": "Conversation folder no longer exists.",
	}[code]
	if message == "" {
		message = "Runtime operation could not be verified."
	}
	retry := "none"
	if outcome == "unknown" {
		retry = "reconcile"
	}
	if code == "busy" || code == "unavailable" {
		retry = "observe"
	}
	switch code {
	case "permission", "unsupported", "conflict", "timeout", "exited", "folder_missing":
		retry = "user"
	}
	return &RuntimeError{Code: code, Outcome: outcome, Retry: retry, Message: message}
}

func WriteAPIResponse(w io.Writer, response APIResponse) error {
	b, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if len(b) > APIOutputBytes {
		response.OK, response.Result, response.Error = false, nil, APIError("overflow", "unknown")
		b, err = json.Marshal(response)
		if err != nil {
			return err
		}
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func apiBuild() RuntimeBuild {
	revision := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revision = setting.Value
			}
			if setting.Key == "vcs.modified" && setting.Value == "true" {
				revision += "+dirty"
			}
		}
	}
	return RuntimeBuild{Protocol: 1, Version: "session-api-v1", Revision: revision}
}

func apiProbe() RuntimeCapabilities {
	providers := []RuntimeProvider{}
	for _, a := range agent.All() {
		if p, ok := a.(agent.SessionAPIProvider); ok {
			providers = append(providers, RuntimeProvider{Kind: string(a.Kind()), Features: p.SessionAPIFeatures()})
		}
	}
	return RuntimeCapabilities{Protocol: 1, Build: apiBuild(), Features: []string{"initialize", "inspect", "list_owned", "prepare", "conditional_send", "receipts", "control_snapshot", "events", "structured_answers", "interrupt", "stop", "failed_phase", "delete"}, Providers: providers}
}

func apiValidRequest(r APIRequest) bool {
	return r.Protocol == 1 && runtimeUUID(r.RequestID) && len(r.Params) > 0 && len(r.Params) <= APIInputBytes && json.Valid(r.Params)
}

func apiFailure(r APIRequest, code, outcome string) APIResponse {
	return APIResponse{Protocol: 1, RequestID: r.RequestID, Error: APIError(code, outcome)}
}

func apiSuccess(r APIRequest, result any) APIResponse {
	return APIResponse{Protocol: 1, RequestID: r.RequestID, OK: true, Result: result}
}

func apiSocket(id string) string { return filepath.Join(dir(id), "api.sock") }

// apiHostID is whether id names one folder under Root() and nothing above
// or beside it: the only kind of folder a removal may take.
func apiHostID(id string) bool {
	return runtimeToken(id, 128) && id != "." && id != ".." && !strings.ContainsAny(id, `/\`)
}

// apiFolderMissing is whether the folder an agent would start in is gone.
// Starting there would fail with no word of why; refusing beforehand is
// definite, and the owner knows what to do about its folder.
func apiFolderMissing(cwd string) bool {
	st, err := os.Stat(cwd)
	return err != nil || !st.IsDir()
}

// APIExecute dispatches supported commands. It never trusts an unrelated live
// process solely because a shortened ID or folder happens to match.
func APIExecute(ctx context.Context, r APIRequest) APIResponse {
	if !apiValidRequest(r) {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	switch r.Method {
	case "probe":
		return apiSuccess(r, apiProbe())
	case "initialize":
		return apiInitialize(ctx, r)
	case "inspect":
		var lookup RuntimeLookup
		if json.Unmarshal(r.Params, &lookup) != nil || lookup.Validate() != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		return apiInspect(ctx, r, lookup)
	case "list_owned":
		var request RuntimeListOwnedRequest
		if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		return apiListOwned(ctx, r, request)
	case "receipt":
		var lookup RuntimeReceiptLookup
		if json.Unmarshal(r.Params, &lookup) != nil || lookup.Validate() != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		return apiReadReceipt(ctx, r, lookup)
	case "delete":
		var request RuntimeDeleteRequest
		if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		return apiDelete(ctx, r, request)
	default:
		var target struct {
			Expected RuntimeIdentity `json:"expected"`
		}
		if json.Unmarshal(r.Params, &target) != nil || target.Expected.Validate(false) != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		return apiCall(ctx, target.Expected.HostID, r)
	}
}

func apiCall(ctx context.Context, id string, r APIRequest) APIResponse {
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", apiSocket(id))
	if err != nil {
		return apiFailure(r, "unavailable", "not_applied")
	}
	defer c.Close()
	done := make(chan struct{})
	defer close(done)
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(deadline)
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-done:
		}
	}()
	b, err := json.Marshal(r)
	if err != nil || len(b) > APIInputBytes {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	if _, err = c.Write(append(b, '\n')); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	line, err := apiReadLine(bufio.NewReader(c))
	if err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	var response APIResponse
	if json.Unmarshal(line, &response) != nil || response.Protocol != 1 || response.RequestID != r.RequestID {
		return apiFailure(r, "unknown", "unknown")
	}
	return response
}

func apiReadLine(r *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(out)+len(part) > APIOutputBytes {
			return nil, errors.New("overflow")
		}
		out = append(out, part...)
		if err == nil {
			return out, nil
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
	}
}

func apiReadState(id string) (RuntimeState, error) {
	var out RuntimeState
	b, err := apiReadFile(filepath.Join(dir(id), "api-state.json"), APIOutputBytes)
	if err != nil {
		return out, err
	}
	if json.Unmarshal(b, &out) != nil || out.Identity.Validate(false) != nil {
		return out, errors.New("invalid state")
	}
	return out, nil
}

func apiReadFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("runtime file exceeds limit")
	}
	return b, nil
}

func apiMatches(state RuntimeState, lookup RuntimeLookup) bool {
	return state.Identity.ConversationID == lookup.ConversationID && state.Identity.Metadata == lookup.Metadata && (lookup.HostID == "" || lookup.HostID == state.Identity.HostID)
}

func apiInspect(ctx context.Context, r APIRequest, lookup RuntimeLookup) APIResponse {
	id := lookup.HostID
	if id == "" {
		id = ShortID(lookup.ConversationID)
	}
	stored, err := apiReadState(id)
	if err != nil {
		if !os.IsNotExist(err) {
			return apiFailure(r, "unavailable", "not_applied")
		}
		if cfg, e := ReadConfig(id); e == nil {
			if cfg.SessionID != lookup.ConversationID || !apiMetadataMatches(cfg.Meta, lookup.Metadata) || !cfg.PublicAPI {
				return apiFailure(r, "conflict", "not_applied")
			}
			return apiFailure(r, "unavailable", "not_applied")
		} else if !os.IsNotExist(e) {
			return apiFailure(r, "unavailable", "not_applied")
		}
		return apiSuccess(r, RuntimeInspection{Status: "missing"})
	}
	if !apiMatches(stored, lookup) {
		return apiFailure(r, "conflict", "not_applied")
	}
	info, err := ReadInfo(id)
	if err != nil {
		return apiFailure(r, "unavailable", "not_applied")
	}
	if !Alive(info.HostPID) {
		status := "missing"
		if stored.Phase == "stopped" || stored.Phase == "failed" {
			status = stored.Phase // its last word stands: the owner reads why
		}
		return apiSuccess(r, RuntimeInspection{Status: status, State: &stored})
	}
	return apiCall(ctx, id, r)
}

// apiDelete removes an owned host: the live one is told to stop first, then
// its folder and scratch go once its process has. A host already gone is
// confirmed too, so a removal the caller lost the answer to can be repeated.
// Only a public host that names this conversation and owner is ever taken.
func apiDelete(ctx context.Context, r APIRequest, request RuntimeDeleteRequest) APIResponse {
	id := request.Expected.HostID
	if id == "" {
		id = ShortID(request.Expected.ConversationID)
	}
	if !apiHostID(id) {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	identity := RuntimeIdentity{ConversationID: request.Expected.ConversationID, HostID: id, Metadata: request.Expected.Metadata}
	receipt := RuntimeReceipt{OperationID: request.OperationID, Identity: identity, Kind: "delete", Outcome: "confirmed"}
	cfg, err := ReadConfig(id)
	if os.IsNotExist(err) {
		return apiSuccess(r, receipt) // nothing of this conversation's to take
	}
	if err != nil {
		return apiFailure(r, "unavailable", "not_applied")
	}
	if cfg.ID != id || cfg.SessionID != request.Expected.ConversationID || !apiMetadataMatches(cfg.Meta, request.Expected.Metadata) || !cfg.PublicAPI {
		return apiFailure(r, "conflict", "not_applied")
	}
	if state, err := apiReadState(id); err == nil && apiMatches(state, request.Expected) {
		receipt.Identity.HostIncarnation = state.Identity.HostIncarnation
	}
	if info, err := ReadInfo(id); err == nil && Alive(info.HostPID) {
		if response := apiCall(ctx, id, r); !response.OK {
			return response
		}
		for Alive(info.HostPID) {
			select {
			case <-ctx.Done():
				return apiFailure(r, "unknown", "unknown") // stopped, its folder still here
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	if tmp := TempDir(id); IsTempDir(tmp) {
		_ = os.RemoveAll(tmp)
	}
	if err := os.RemoveAll(dir(id)); err != nil {
		return apiFailure(r, "permission", "unknown")
	}
	return apiSuccess(r, receipt)
}

func apiInitialize(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimeInitializeRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	if request.Expected != nil {
		return apiCall(ctx, request.Expected.HostID, r)
	}
	id := ShortID(request.ConversationID)
	if err := os.MkdirAll(dir(id), 0700); err != nil {
		return apiFailure(r, "permission", "not_applied")
	}
	lock, err := os.OpenFile(filepath.Join(dir(id), "api-start.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return apiFailure(r, "permission", "not_applied")
	}
	defer lock.Close()
	for syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		select {
		case <-ctx.Done():
			return apiFailure(r, "cancelled", "not_applied")
		case <-time.After(20 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	cwd := request.Cwd
	if cfg, e := ReadConfig(id); e == nil {
		if cfg.SessionID != request.ConversationID || !apiMetadataMatches(cfg.Meta, request.Metadata) || !cfg.PublicAPI {
			return apiFailure(r, "conflict", "not_applied")
		}
		if info, e := ReadInfo(id); e == nil && Alive(info.HostPID) {
			return apiCall(ctx, id, r)
		}
		if apiReplaysInitialize(id, request.OperationID, r.Params) {
			cwd = cfg.Cwd
		}
	}
	provider, ok := agent.As[agent.SessionAPIProvider](agent.Kind(request.Provider))
	if !ok {
		return apiFailure(r, "unsupported", "not_applied")
	}
	if apiFolderMissing(cwd) {
		return apiFailure(r, "folder_missing", "not_applied")
	}
	cfg := Config{ID: id, SessionID: request.ConversationID, Cwd: cwd, Kind: request.Provider, Resume: request.Resume, ForkFrom: request.ForkFrom, Name: "External conversation", Binary: request.EngineBinary, Model: request.Settings.Model, Effort: request.Settings.Effort, PermissionMode: provider.SessionAPIEngineMode(request.Settings.Mode), PublicAPI: true,
		Account: provider.SessionAPIProfile(), Meta: apiMetadata(request.Metadata)}
	if _, err := Spawn(cfg); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	return apiCall(ctx, id, r)
}

func apiMetadata(o RuntimeOwnership) map[string]string {
	return map[string]string{"amtana_metadata_version": o.Version, "amtana_installation": o.Installation, "amtana_chat": o.Chat, "amtana_operation": o.Operation}
}
func apiMetadataMatches(m map[string]string, o RuntimeOwnership) bool {
	for k, v := range apiMetadata(o) {
		if m[k] != v {
			return false
		}
	}
	return true
}
