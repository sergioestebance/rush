package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
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

func (h publicAPIHarness) initialize(t *testing.T, chat string) host.RuntimeState {
	t.Helper()
	conversation, _ := host.NewSessionID()
	installation, _ := host.NewSessionID()
	operation, _ := host.NewSessionID()
	request := host.RuntimeInitializeRequest{ConversationID: conversation, OperationID: operation, Metadata: host.RuntimeOwnership{Version: "1", Installation: installation, Chat: chat, Operation: operation}, Cwd: h.dir, Provider: "claude", EngineBinary: h.engine, Settings: host.RuntimeSettings{Model: "opus", Effort: "medium", Mode: "auto"}}
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
