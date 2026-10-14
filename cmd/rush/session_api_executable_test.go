package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/0xdeafcafe/rush/internal/host"
)

// This is a synthetic provider behind the REAL compiled CLI, detached host,
// provider adapter and writer. It deliberately makes no account/model calls.
const publicAPIEngine = `import sys,json,os
args=sys.argv[1:]
def arg(name,default=''):
    return args[args.index(name)+1] if name in args else default
sid=arg('--session-id',arg('--resume'))
effort=arg('--effort')
mode=arg('--permission-mode')
def emit(v): print(json.dumps(v),flush=True)
def log(v):
    with open(os.environ['API_ENGINE_LOG'],'a') as f: f.write(json.dumps(v)+'\n')
emit({'type':'system','subtype':'init','session_id':sid,'cwd':os.getcwd(),'model':arg('--model'),'effort':arg('--effort'),'permissionMode':arg('--permission-mode'),'tools':['Bash','AskUserQuestion']})
for line in sys.stdin:
    v=json.loads(line)
    if v.get('type')=='control_request':
        subtype=v['request']['subtype']
        body={}
        if subtype=='apply_flag_settings': effort=v['request']['settings']['effortLevel']
        elif subtype=='set_permission_mode': mode=v['request']['mode'];body={'mode':mode}
        elif subtype=='get_settings': body={'applied':{'model':arg('--model'),'effort':effort}}
        elif subtype=='get_status': body={'sections':[{'title':'Session','rows':[{'label':'Session ID','value':sid},{'label':'cwd','value':os.getcwd()}]}]}
        emit({'type':'control_response','response':{'subtype':'success','request_id':v['request_id'],'response':body}})
        if subtype=='interrupt': emit({'type':'result','subtype':'success','result':'interrupted'})
    elif v.get('type')=='user':
        text=v['message']['content']; log({'text':text})
        if text=='hold': continue
        if text=='question':
            emit({'type':'control_request','request_id':'request-1','request':{'subtype':'can_use_tool','tool_name':'AskUserQuestion','tool_use_id':'call-1','input':{'questions':[{'question':'Choose','header':'Choice','options':[{'label':'A','description':'First'},{'label':'B','description':'Second'}],'multiSelect':False}]}}})
        elif text=='approval':
            emit({'type':'control_request','request_id':'request-1','request':{'subtype':'can_use_tool','tool_name':'Bash','tool_use_id':'call-1','input':{'command':'synthetic'}}})
        else: emit({'type':'result','subtype':'success','result':'done'})
    elif v.get('type')=='control_response':
        log({'answer':v['response']})
        emit({'type':'result','subtype':'success','result':'answered'})
`

type publicAPIHarness struct{ binary, dir, engine, log string }

func newPublicAPIHarness(t *testing.T) publicAPIHarness {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("synthetic provider requires python3")
	}
	dir, err := os.MkdirTemp("/tmp", "api-v1-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	for _, d := range []string{"home/.claude", "state", "cache", "work"} {
		if err = os.MkdirAll(filepath.Join(dir, d), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HOME", filepath.Join(dir, "home"))
	t.Setenv("RUSH_HOME", filepath.Join(dir, "state"))
	t.Setenv("RUSH_CACHE", filepath.Join(dir, "cache"))
	h := publicAPIHarness{binary: filepath.Join(dir, "runtime"), dir: filepath.Join(dir, "work"), engine: filepath.Join(dir, "engine"), log: filepath.Join(dir, "engine.log")}
	t.Setenv("API_ENGINE_LOG", h.log)
	if err = os.WriteFile(h.engine, []byte("#!"+python+"\n"+publicAPIEngine), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", h.binary, ".")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build real runtime: %v\n%s", err, b)
	}
	return h
}

func (h publicAPIHarness) call(t *testing.T, method string, params any, result any) host.APIResponse {
	t.Helper()
	requestID, _ := host.NewSessionID()
	b, err := json.Marshal(map[string]any{"protocol": 1, "requestId": requestID, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.binary, "session", "api", "--protocol", "1")
	cmd.Stdin = bytes.NewReader(b)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err = cmd.Run(); err != nil {
		t.Fatalf("%s failed: %v, %s, %s", method, err, out.String(), stderr.String())
	}
	var response host.APIResponse
	if err = json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatalf("%s response: %v %s", method, err, out.String())
	}
	if response.OK && result != nil {
		value, _ := json.Marshal(response.Result)
		if err = json.Unmarshal(value, result); err != nil {
			t.Fatal(err)
		}
	}
	return response
}

func (h publicAPIHarness) request(chat string) host.RuntimeInitializeRequest {
	conversation, _ := host.NewSessionID()
	installation, _ := host.NewSessionID()
	operation, _ := host.NewSessionID()
	return host.RuntimeInitializeRequest{ConversationID: conversation, OperationID: operation, Metadata: host.RuntimeOwnership{Version: "1", Installation: installation, Chat: chat, Operation: operation}, Cwd: h.dir, Provider: "claude", EngineBinary: h.engine, Settings: host.RuntimeSettings{Model: "opus", Effort: "medium", Mode: "auto"}}
}

func (h publicAPIHarness) initialize(t *testing.T, chat string) host.RuntimeState {
	t.Helper()
	request := h.request(chat)
	var state host.RuntimeState
	response := h.call(t, "initialize", request, &state)
	if !response.OK || state.ValidateReady(request) != nil {
		t.Fatalf("not engine-ready: %+v %+v", response, state)
	}
	t.Cleanup(func() {
		expected := state.Identity
		expected.AgentIncarnation = ""
		op, _ := host.NewSessionID()
		h.call(t, "stop", host.RuntimeStopRequest{OperationID: op, Expected: expected}, nil)
	})
	return state
}

func (h publicAPIHarness) inspect(t *testing.T, id host.RuntimeIdentity) host.RuntimeState {
	t.Helper()
	var inspection host.RuntimeInspection
	response := h.call(t, "inspect", host.RuntimeLookup{ConversationID: id.ConversationID, HostID: id.HostID, Metadata: id.Metadata}, &inspection)
	if !response.OK || inspection.State == nil {
		t.Fatalf("inspect: %+v", response)
	}
	return *inspection.State
}

func TestPublicExecutableIsolatesChatsAndDeduplicatesDelivery(t *testing.T) {
	h := newPublicAPIHarness(t)
	a := h.initialize(t, "chat-a")
	b := h.initialize(t, "chat-b")
	if a.Identity.HostID == b.Identity.HostID || a.Identity.ConversationID == b.Identity.ConversationID {
		t.Fatal("folder merged conversations")
	}
	ownedRequest := host.RuntimeListOwnedRequest{Installation: a.Identity.Metadata.Installation, Limit: 10}
	var owned host.RuntimeOwnedPage
	if response := h.call(t, "list_owned", ownedRequest, &owned); !response.OK || owned.Validate(ownedRequest) != nil ||
		len(owned.Items) != 1 || owned.Items[0].HostID != a.Identity.HostID || owned.Items[0].Status != "found" {
		t.Fatalf("owned host discovery mixed installations or lost a live host: %+v %+v", response, owned)
	}
	if data, _ := os.ReadFile(h.log); len(data) != 0 {
		t.Fatal("initialization delivered a prompt")
	}
	operation, _ := host.NewSessionID()
	send := host.RuntimeSendRequest{OperationID: operation, Expected: a.Identity, RequireTurnState: "idle", Settings: a.Settings, Text: "hold"}
	var first, duplicate host.RuntimeReceipt
	if response := h.call(t, "send", send, &first); !response.OK || first.Validate(a.Identity, operation, "send") != nil || first.Outcome != "delivered" {
		t.Fatalf("receipt: %+v %+v", response, first)
	}
	if response := h.call(t, "send", send, &duplicate); !response.OK || duplicate.TurnID != first.TurnID {
		t.Fatal("repeated operation did not return receipt")
	}
	otherOperation, _ := host.NewSessionID()
	send.OperationID = otherOperation
	if response := h.call(t, "send", send, nil); response.OK || response.Error == nil || response.Error.Code != "busy" {
		t.Fatalf("active turn accepted: %+v", response)
	}
	data, _ := os.ReadFile(h.log)
	if strings.Count(string(data), `"text": "hold"`) != 1 {
		t.Fatalf("duplicate provider delivery: %s", data)
	}
	var receipt host.RuntimeReceipt
	if response := h.call(t, "receipt", host.RuntimeReceiptLookup{OperationID: operation, Expected: host.RuntimeLookup{ConversationID: a.Identity.ConversationID, HostID: a.Identity.HostID, Metadata: a.Identity.Metadata}}, &receipt); !response.OK || receipt.Outcome != "delivered" {
		t.Fatalf("receipt lookup: %+v", response)
	}
	interruptID, _ := host.NewSessionID()
	if response := h.call(t, "interrupt", host.RuntimeInterruptRequest{OperationID: interruptID, Expected: a.Identity, TurnID: first.TurnID}, &receipt); !response.OK || receipt.Outcome != "confirmed" {
		t.Fatalf("interrupt: %+v", response)
	}
}

func TestPublicExecutableSnapshotsAndNativeQuestions(t *testing.T) {
	h := newPublicAPIHarness(t)
	state := h.initialize(t, "questions")
	operation, _ := host.NewSessionID()
	var receipt host.RuntimeReceipt
	if response := h.call(t, "send", host.RuntimeSendRequest{OperationID: operation, Expected: state.Identity, RequireTurnState: "idle", Settings: state.Settings, Text: "question"}, &receipt); !response.OK {
		t.Fatalf("send: %+v", response)
	}
	requestID, _ := host.NewSessionID()
	body, _ := json.Marshal(map[string]any{"protocol": 1, "requestId": requestID, "method": "subscribe", "params": host.RuntimeSubscribeRequest{Expected: state.Identity}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	watch := exec.CommandContext(ctx, h.binary, "session", "events", "--protocol", "1")
	watch.Stdin = bytes.NewReader(body)
	out, err := watch.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = watch.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); watch.Wait() }()
	reader := bufio.NewReader(out)
	var snapshot host.RuntimeEvent
	line, err := reader.ReadBytes('\n')
	if err != nil || json.Unmarshal(line, &snapshot) != nil || snapshot.Type != "snapshot" || snapshot.Snapshot == nil {
		t.Fatalf("missing snapshot: %s %v", line, err)
	}
	if err = snapshot.Snapshot.Validate(state.Identity); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); len(snapshot.Snapshot.Pending) == 0 && time.Now().Before(deadline); {
		line, err = reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var update host.RuntimeEvent
		if json.Unmarshal(line, &update) != nil {
			t.Fatal("invalid event")
		}
		if update.Type == "request" {
			snapshot.Snapshot.Pending = append(snapshot.Snapshot.Pending, *update.Request)
		}
	}
	if len(snapshot.Snapshot.Pending) != 1 {
		t.Fatalf("incomplete requests: %+v", snapshot)
	}
	pending := snapshot.Snapshot.Pending[0]
	answerID, _ := host.NewSessionID()
	answer := host.RuntimeAnswerRequest{OperationID: answerID, Expected: state.Identity, Request: host.RuntimeRequestIdentity{ID: pending.ID, AgentIncarnation: pending.AgentIncarnation}, Decision: "allow", Answers: map[string]string{pending.Questions[0].ID: "A"}}
	if response := h.call(t, "answer", answer, &receipt); !response.OK || receipt.RequestID != pending.ID || receipt.Outcome != "confirmed" {
		t.Fatalf("native answer: %+v %+v", response, receipt)
	}
	data, _ := os.ReadFile(h.log)
	if !strings.Contains(string(data), `"answers": {"Choose": "A"}`) {
		t.Fatalf("native question answer did not reach provider: %s", data)
	}
	previous := state.Identity.AgentIncarnation
	state = h.inspect(t, state.Identity)
	op, _ := host.NewSessionID()
	var prepared host.RuntimeState
	if response := h.call(t, "prepare", host.RuntimePrepareRequest{OperationID: op, Expected: state.Identity, Settings: host.RuntimeSettings{Model: "sonnet", Effort: "high", Mode: "ask"}}, &prepared); !response.OK || prepared.Settings.Mode != "ask" || prepared.Identity.AgentIncarnation == previous {
		t.Fatalf("effective settings replacement: %+v %+v", response, prepared)
	}
	answer.OperationID, _ = host.NewSessionID()
	if response := h.call(t, "answer", answer, nil); response.OK || response.Error == nil || response.Error.Code != "stale" {
		t.Fatalf("old agent answer accepted: %+v", response)
	}
}

// A host follows its agent to another folder, while its owner only knows the
// folder it first asked for. Repeating that exact initialization, after a
// restart or after the host stopped, resumes where the agent went.
func TestPublicExecutableReplayKeepsTheFollowedFolder(t *testing.T) {
	h := newPublicAPIHarness(t)
	request := h.request("moved")
	var state host.RuntimeState
	if response := h.call(t, "initialize", request, &state); !response.OK || state.ValidateReady(request) != nil {
		t.Fatalf("not engine-ready: %+v %+v", response, state)
	}
	id := host.ShortID(request.ConversationID)
	moved := filepath.Join(filepath.Dir(h.dir), "moved")
	if err := os.MkdirAll(moved, 0700); err != nil {
		t.Fatal(err)
	}
	for _, restart := range []bool{true, false} {
		expected := state.Identity
		expected.AgentIncarnation = ""
		op, _ := host.NewSessionID()
		if response := h.call(t, "stop", host.RuntimeStopRequest{OperationID: op, Expected: expected}, nil); !response.OK {
			t.Fatalf("stop: %+v", response)
		}
		for deadline := time.Now().Add(10 * time.Second); ; {
			if info, err := host.ReadInfo(id); err != nil || !host.Alive(info.HostPID) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("host did not exit")
			}
			time.Sleep(50 * time.Millisecond)
		}
		// What the host saves when its agent moves.
		cfg, err := host.ReadConfig(id)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Cwd = moved
		b, _ := json.Marshal(cfg)
		if err = os.WriteFile(filepath.Join(host.Root(), id, "config.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		if restart {
			if out, err := exec.Command(h.binary, "session", "start", "--session-id", request.ConversationID, "--resume", "--json").CombinedOutput(); err != nil {
				t.Fatalf("restart: %v %s", err, out)
			}
		}
		if response := h.call(t, "initialize", request, &state); !response.OK || state.Phase != "ready" || state.Cwd != moved {
			t.Fatalf("restart %v: replay lost the followed folder: %+v %+v", restart, response, state)
		}
	}
	// Only the recorded request earns the followed folder: another folder is
	// still refused.
	changed := request
	changed.Cwd = filepath.Dir(h.dir)
	if response := h.call(t, "initialize", changed, nil); response.OK || response.Error == nil || response.Error.Code != "conflict" {
		t.Fatalf("a changed initialization was accepted: %+v", response)
	}
	t.Cleanup(func() {
		expected := state.Identity
		expected.AgentIncarnation = ""
		op, _ := host.NewSessionID()
		h.call(t, "stop", host.RuntimeStopRequest{OperationID: op, Expected: expected}, nil)
	})
}

// The daemon hands each image over as a file it deletes once the message
// leaves its queue. The runtime reads and checks them all before the provider
// sees anything, so a bad file starts no turn, and never reads them again.
func TestPublicExecutableSendsImagesAndRefusesBadFiles(t *testing.T) {
	h := newPublicAPIHarness(t)
	var capabilities host.RuntimeCapabilities
	if response := h.call(t, "probe", map[string]any{}, &capabilities); !response.OK {
		t.Fatalf("probe: %+v", response)
	}
	imageInput := false
	for _, p := range capabilities.Providers {
		imageInput = imageInput || p.Kind == "claude" && slices.Contains(p.Features, "image_input")
	}
	if !imageInput {
		t.Fatalf("claude does not report image_input: %+v", capabilities.Providers)
	}

	state := h.initialize(t, "images")
	attach := filepath.Join(filepath.Dir(h.dir), "attach")
	if err := os.MkdirAll(attach, 0700); err != nil {
		t.Fatal(err)
	}
	png := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n', 'p', 'n', 'g'}
	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 'j', 'p', 'e', 'g'}
	pngPath, jpegPath := filepath.Join(attach, "1.png"), filepath.Join(attach, "2.jpg")
	for path, b := range map[string][]byte{pngPath: png, jpegPath: jpeg} {
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	lookup := host.RuntimeLookup{ConversationID: state.Identity.ConversationID, HostID: state.Identity.HostID, Metadata: state.Identity.Metadata}
	for _, tc := range []struct {
		name   string
		images []host.RuntimeImage
	}{
		{"missing file", []host.RuntimeImage{{Path: pngPath, MediaType: "image/png"}, {Path: filepath.Join(attach, "gone.jpg"), MediaType: "image/jpeg"}}},
		{"bytes do not match the type", []host.RuntimeImage{{Path: jpegPath, MediaType: "image/png"}}},
		{"unsupported type", []host.RuntimeImage{{Path: pngPath, MediaType: "image/gif"}}},
	} {
		operation, _ := host.NewSessionID()
		send := host.RuntimeSendRequest{OperationID: operation, Expected: state.Identity, RequireTurnState: "idle", Settings: state.Settings, Text: "[Image #1]", Images: tc.images}
		if response := h.call(t, "send", send, nil); response.OK || response.Error == nil || response.Error.Code != "invalid_input" || response.Error.Outcome != "not_applied" {
			t.Fatalf("%s: %+v", tc.name, response)
		}
		var receipt host.RuntimeReceipt
		if response := h.call(t, "receipt", host.RuntimeReceiptLookup{OperationID: operation, Expected: lookup}, &receipt); !response.OK || receipt.Outcome != "not_known" {
			t.Fatalf("%s left a receipt: %+v %+v", tc.name, response, receipt)
		}
		if turn := h.inspect(t, state.Identity).Turn; turn.State != "idle" || turn.ID != "" {
			t.Fatalf("%s started a turn: %+v", tc.name, turn)
		}
		if data, _ := os.ReadFile(h.log); len(data) != 0 {
			t.Fatalf("%s reached the provider: %s", tc.name, data)
		}
	}

	operation, _ := host.NewSessionID()
	send := host.RuntimeSendRequest{OperationID: operation, Expected: state.Identity, RequireTurnState: "idle", Settings: state.Settings,
		Text: "First [Image #1] then [Image #2]", Images: []host.RuntimeImage{{Path: pngPath, MediaType: "image/png"}, {Path: jpegPath, MediaType: "image/jpeg"}}}
	var first, duplicate host.RuntimeReceipt
	if response := h.call(t, "send", send, &first); !response.OK || first.Validate(state.Identity, operation, "send") != nil || first.Outcome != "delivered" {
		t.Fatalf("image send: %+v %+v", response, first)
	}
	// The daemon deletes the files once delivered; a repeat still finds its receipt.
	os.Remove(pngPath)
	os.Remove(jpegPath)
	if response := h.call(t, "send", send, &duplicate); !response.OK || duplicate.TurnID != first.TurnID {
		t.Fatalf("repeated image send: %+v", response)
	}
	data, _ := os.ReadFile(h.log)
	if bytes.Count(data, []byte("\n")) != 1 {
		t.Fatalf("want one provider message: %s", data)
	}
	var logged struct {
		Text []map[string]any `json:"text"`
	}
	if err := json.Unmarshal(data, &logged); err != nil {
		t.Fatalf("provider content is not blocks: %v %s", err, data)
	}
	image := func(mediaType string, b []byte) map[string]any {
		return map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": mediaType, "data": base64.StdEncoding.EncodeToString(b)}}
	}
	want := []map[string]any{
		{"type": "text", "text": "First [Image #1]"}, image("image/png", png),
		{"type": "text", "text": " then [Image #2]"}, image("image/jpeg", jpeg),
	}
	if !reflect.DeepEqual(logged.Text, want) {
		t.Fatalf("provider content = %+v\nwant %+v", logged.Text, want)
	}
}
