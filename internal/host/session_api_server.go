package host

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/0xdeafcafe/rush/internal/agent"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

type apiControl struct {
	s                  *server
	listener           net.Listener
	mutation           sync.Mutex
	state              RuntimeState
	pending            map[string]RuntimePendingRequest
	questions          map[string]event.Question
	watchers           map[*apiWatcher]struct{}
	ready              bool
	confirming         bool
	handshakeConfirmed bool
	failure            *RuntimeError
	hadAgent           bool
	hasHistory         bool
	stopped            bool
	sequence           uint64
}

type apiWatcher struct {
	out   chan []byte
	done  chan struct{}
	once  sync.Once
	bytes int
}

func apiModel(kind, model string) string {
	if p, ok := agent.As[agent.SessionAPIProvider](agent.Kind(kind)); ok {
		return p.SessionAPIModel(model)
	}
	return model
}

func apiMode(kind, mode string) string {
	if p, ok := agent.As[agent.SessionAPIProvider](agent.Kind(kind)); ok {
		return p.SessionAPIMode(mode)
	}
	return mode
}

func apiEngineMode(kind, mode string) string {
	if p, ok := agent.As[agent.SessionAPIProvider](agent.Kind(kind)); ok {
		return p.SessionAPIEngineMode(mode)
	}
	return mode
}

func (w *apiWatcher) close() { w.once.Do(func() { close(w.done) }) }

func newAPIControl(s *server) *apiControl {
	hostID, _ := NewSessionID()
	o := RuntimeOwnership{Version: s.cfg.Meta["amtana_metadata_version"], Installation: s.cfg.Meta["amtana_installation"], Chat: s.cfg.Meta["amtana_chat"], Operation: s.cfg.Meta["amtana_operation"]}
	return &apiControl{s: s, hasHistory: s.cfg.Resume || s.cfg.ForkFrom != "", pending: map[string]RuntimePendingRequest{}, questions: map[string]event.Question{}, watchers: map[*apiWatcher]struct{}{}, state: RuntimeState{Identity: RuntimeIdentity{ConversationID: s.cfg.SessionID, HostID: s.cfg.ID, HostIncarnation: hostID, Metadata: o}, Build: apiBuild(), Cwd: s.cfg.Cwd, Phase: "host_starting", Settings: RuntimeSettings{Model: s.cfg.Model, Effort: s.cfg.Effort, Mode: apiMode(s.cfg.Kind, s.cfg.PermissionMode)}, Turn: RuntimeTurn{State: "idle"}}}
}

func (s *server) listenAPI() error {
	_ = os.Remove(apiSocket(s.cfg.ID))
	l, err := net.Listen("unix", apiSocket(s.cfg.ID))
	if err != nil {
		return err
	}
	if err = os.Chmod(apiSocket(s.cfg.ID), 0600); err != nil {
		l.Close()
		return err
	}
	s.api.listener = l
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go s.serveAPI(c)
		}
	}()
	return nil
}

func (s *server) serveAPI(c net.Conn) {
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
	line, err := apiReadLine(bufio.NewReader(c))
	if err != nil {
		return
	}
	var r APIRequest
	if json.Unmarshal(line, &r) != nil || !apiValidRequest(r) {
		_ = WriteAPIResponse(c, apiFailure(r, "invalid_input", "not_applied"))
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	if r.Method == "subscribe" {
		s.api.subscribe(r, c)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response := s.api.execute(ctx, r)
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = WriteAPIResponse(c, response)
	if r.Method == "stop" && response.OK {
		s.stopOnce.Do(func() { close(s.quit) })
	}
}

func (a *apiControl) agentStarted() {
	id, _ := NewSessionID()
	a.state.Identity.AgentIncarnation = id
	a.hadAgent = true
	a.state.LastConfirmedDelivery = nil
	a.ready = false
	a.confirming = false
	a.handshakeConfirmed = false
	a.failure = nil
	a.pending = map[string]RuntimePendingRequest{}
	a.questions = map[string]event.Question{}
}

func (a *apiControl) agentGone() {
	a.state.Identity.AgentIncarnation = ""
	a.state.LastConfirmedDelivery = nil
	a.ready = false
	a.confirming = false
	a.handshakeConfirmed = false
	a.pending = map[string]RuntimePendingRequest{}
	a.questions = map[string]event.Question{}
}

// changed runs under the host mutex, shared with legacy TUI changes.
func (a *apiControl) changed() {
	configured := RuntimeSettings{Model: a.s.cfg.Model, Effort: a.s.cfg.Effort, Mode: apiMode(a.s.cfg.Kind, a.s.cfg.PermissionMode)}
	if a.ready && configured != a.state.Settings {
		a.ready = false
	}
	a.sequence++
	a.state.Cursor = RuntimeCursor{HostIncarnation: a.state.Identity.HostIncarnation, Sequence: a.sequence}
	a.state.Cwd = a.s.cfg.Cwd
	state := a.s.info.State
	if state == "blocked" {
		state = "waiting"
	}
	if state == "stopped" {
		state = "idle"
	}
	if state == "starting" {
		state = "unknown"
	}
	if state == "working" && a.state.Turn.State == "idle" {
		id, _ := NewSessionID()
		a.state.Turn.ID = id
		a.state.LastConfirmedDelivery = nil
	}
	a.state.Turn.State = state
	a.state.SettingsConfirmed = a.ready
	switch {
	case a.stopped:
		a.state.Phase = "stopped"
	case a.s.conn == nil && a.hadAgent:
		a.state.Phase = "sleeping"
	case a.s.conn == nil:
		a.state.Phase = "host_starting"
	case !a.ready:
		a.state.Phase = "agent_starting"
	default:
		a.state.Phase = "ready"
	}
	b, _ := json.Marshal(a.state)
	if len(b) <= APIOutputBytes {
		_ = apiAtomic(filepath.Join(dir(a.s.cfg.ID), "api-state.json"), b)
	}
	a.emit(RuntimeEvent{Type: "state", Identity: a.state.Identity, Cursor: a.state.Cursor, State: &a.state})
}

func (a *apiControl) emit(e RuntimeEvent) {
	b, err := json.Marshal(e)
	if err != nil || len(b) > APIOutputBytes {
		for w := range a.watchers {
			w.close()
		}
		return
	}
	for w := range a.watchers {
		if w.bytes+len(b) > 16<<20 {
			w.close()
			continue
		}
		select {
		case w.out <- b:
			w.bytes += len(b)
		default:
			w.close()
		}
	}
}

func (a *apiControl) onEvent(ev event.Event) {
	switch e := ev.(type) {
	case event.Init:
		// Only provider-reported fields confirm readiness. Config echoes and
		// successful process spawn do not fill missing effective values.
		settings := RuntimeSettings{Model: apiModel(a.s.cfg.Kind, e.Model), Effort: e.Effort, Mode: apiMode(a.s.cfg.Kind, e.Mode)}
		if settings.Effort == "" && a.ready && settings.Model == a.state.Settings.Model && settings.Mode == a.state.Settings.Mode {
			settings.Effort = a.state.Settings.Effort
		}
		wanted := RuntimeSettings{Model: a.s.cfg.Model, Effort: a.s.cfg.Effort, Mode: apiMode(a.s.cfg.Kind, a.s.cfg.PermissionMode)}
		a.ready = false
		if a.handshakeConfirmed && e.SessionID == a.state.Identity.ConversationID && e.Cwd == a.s.cfg.Cwd && settings == wanted && settings.Validate() == nil {
			a.state.Settings = settings
			a.ready = true
		}
	case event.Approval:
		input, _ := json.Marshal(e.Call.Input)
		p := RuntimePendingRequest{ID: e.ID, AgentIncarnation: a.state.Identity.AgentIncarnation, Kind: "approval", Tool: e.Call.Name, CallID: e.Call.ID, Input: input, Reason: e.Reason}
		a.addPending(p)
	case event.Question:
		p := RuntimePendingRequest{ID: e.ID, AgentIncarnation: a.state.Identity.AgentIncarnation, Kind: "question", CallID: e.CallID}
		for i, q := range e.Asks {
			id := q.ID
			if id == "" {
				id = "q" + strconv.Itoa(i+1)
			}
			question := RuntimeQuestion{ID: id, Text: q.Text, Multiple: q.Multi, Choices: []RuntimeChoice{}}
			for j, c := range q.Options {
				question.Choices = append(question.Choices, RuntimeChoice{ID: "c" + strconv.Itoa(j+1), Label: c.Label, Description: c.Description})
			}
			p.Questions = append(p.Questions, question)
		}
		a.questions[e.ID] = e
		a.addPending(p)
	case event.ApprovalCancelled:
		a.resolve(e.ID)
	case event.TurnEnd:
		// A request from the completed turn never remains answerable.
		for id := range a.pending {
			a.resolve(id)
		}
	}
}

func (a *apiControl) addPending(p RuntimePendingRequest) {
	a.pending[p.ID] = p
	a.sequence++
	a.state.Cursor = RuntimeCursor{HostIncarnation: a.state.Identity.HostIncarnation, Sequence: a.sequence}
	a.emit(RuntimeEvent{Type: "request", Identity: a.state.Identity, Cursor: a.state.Cursor, Request: &p})
}

func (a *apiControl) resolve(id string) {
	if _, ok := a.pending[id]; !ok {
		return
	}
	delete(a.pending, id)
	delete(a.questions, id)
	a.sequence++
	a.state.Cursor = RuntimeCursor{HostIncarnation: a.state.Identity.HostIncarnation, Sequence: a.sequence}
	a.emit(RuntimeEvent{Type: "resolved", Identity: a.state.Identity, Cursor: a.state.Cursor, Resolved: &RuntimeRequestIdentity{ID: id, AgentIncarnation: a.state.Identity.AgentIncarnation}})
}

func (a *apiControl) snapshot() RuntimeSnapshot {
	pending := make([]RuntimePendingRequest, 0, len(a.pending))
	for _, p := range a.pending {
		pending = append(pending, p)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].ID < pending[j].ID })
	return RuntimeSnapshot{State: a.state, Cursor: a.state.Cursor, Pending: pending}
}

func (a *apiControl) subscribe(r APIRequest, c net.Conn) {
	var request RuntimeSubscribeRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		_ = WriteAPIResponse(c, apiFailure(r, "invalid_input", "not_applied"))
		return
	}
	w := &apiWatcher{out: make(chan []byte, 16), done: make(chan struct{})}
	a.s.mu.Lock()
	if a.state.Identity.Matches(request.Expected) != nil {
		a.s.mu.Unlock()
		_ = WriteAPIResponse(c, apiFailure(r, "stale", "not_applied"))
		return
	}
	snapshot := a.snapshot()
	line, err := json.Marshal(RuntimeEvent{Type: "snapshot", Identity: a.state.Identity, Cursor: a.state.Cursor, Snapshot: &snapshot})
	if err != nil || len(line) > APIOutputBytes {
		a.s.mu.Unlock()
		_ = WriteAPIResponse(c, apiFailure(r, "overflow", "not_applied"))
		return
	}
	a.watchers[w] = struct{}{}
	a.s.mu.Unlock()
	defer func() { a.s.mu.Lock(); delete(a.watchers, w); a.s.mu.Unlock(); w.close() }()
	// Peer closure releases an otherwise quiet control subscription.
	go func() { var b [1]byte; _, _ = c.Read(b[:]); w.close() }()
	write := func(b []byte) bool {
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := c.Write(append(b, '\n'))
		return err == nil
	}
	if !write(line) {
		return
	}
	for {
		select {
		case b := <-w.out:
			a.s.mu.Lock()
			w.bytes -= len(b)
			a.s.mu.Unlock()
			if !write(b) {
				return
			}
		case <-w.done:
			return
		case <-a.s.quit:
			return
		}
	}
}

func (a *apiControl) execute(ctx context.Context, r APIRequest) APIResponse {
	if r.Method == "inspect" {
		var lookup RuntimeLookup
		if json.Unmarshal(r.Params, &lookup) != nil || lookup.Validate() != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		a.s.mu.Lock()
		defer a.s.mu.Unlock()
		if !apiMatches(a.state, lookup) {
			return apiFailure(r, "conflict", "not_applied")
		}
		if a.failure != nil {
			return APIResponse{Protocol: 1, RequestID: r.RequestID, Error: a.failure}
		}
		copy := a.state
		return apiSuccess(r, RuntimeInspection{Status: copy.Phase, State: &copy})
	}
	if r.Method == "receipt" {
		var lookup RuntimeReceiptLookup
		if json.Unmarshal(r.Params, &lookup) != nil || lookup.Validate() != nil {
			return apiFailure(r, "invalid_input", "not_applied")
		}
		a.s.mu.Lock()
		defer a.s.mu.Unlock()
		if !apiMatches(a.state, lookup.Expected) {
			return apiFailure(r, "conflict", "not_applied")
		}
		return a.lookupReceipt(r, lookup.OperationID)
	}
	a.mutation.Lock()
	defer a.mutation.Unlock()
	switch r.Method {
	case "initialize":
		return a.initialize(ctx, r)
	case "prepare":
		return a.prepare(ctx, r)
	case "send":
		return a.send(ctx, r)
	case "answer":
		return a.answer(ctx, r)
	case "interrupt":
		return a.interrupt(ctx, r)
	case "stop":
		return a.stop(ctx, r)
	default:
		return apiFailure(r, "unsupported", "not_applied")
	}
}

func (a *apiControl) waitReady(ctx context.Context) bool {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.s.mu.Lock()
		ready, failed := a.ready, a.failure != nil
		a.s.mu.Unlock()
		if ready {
			return true
		}
		if failed {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-a.s.quit:
			return false
		case <-ticker.C:
		}
	}
}

// Confirmation outlives a timed-out CLI exchange. Each provider request has its
// own deadline; no private prompt is sent while startup is being reconciled.
func (a *apiControl) startConfirmation(operation, kind string) {
	if a.confirming {
		return
	}
	conn := a.s.conn
	provider, ok := conn.(interface {
		InitializeExecution(context.Context, string, string, string) (event.Init, error)
	})
	if !ok {
		a.failure = APIError("unsupported", "unknown")
		return
	}
	a.confirming = true
	a.ready = false
	a.handshakeConfirmed = false
	a.failure = nil
	model, effort, mode := a.s.cfg.Model, a.s.cfg.Effort, a.s.cfg.PermissionMode
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-a.s.quit:
				cancel()
			case <-done:
			}
		}()
		initialized, err := provider.InitializeExecution(ctx, model, effort, mode)
		a.s.mu.Lock()
		defer a.s.mu.Unlock()
		if a.s.conn != conn {
			return
		}
		a.confirming = false
		if err != nil {
			a.failure = APIError("unsupported", "unknown")
			a.s.publish()
			return
		}
		a.handshakeConfirmed = true
		a.s.onAgentEvent(conn, initialized)
		if !a.ready {
			a.failure = APIError("conflict", "unknown")
			a.s.publish()
			return
		}
		if err := a.finish(operation, kind, "confirmed", ""); err != nil {
			a.failure = APIError("unknown", "unknown")
		}
		a.s.publish()
	}()
}

func (a *apiControl) initialize(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimeInitializeRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	a.s.mu.Lock()
	if request.ConversationID != a.state.Identity.ConversationID || request.Metadata != a.state.Identity.Metadata ||
		request.Cwd != a.s.cfg.Cwd && !apiReplaysInitialize(a.s.cfg.ID, request.OperationID, r.Params) {
		a.s.mu.Unlock()
		return apiFailure(r, "conflict", "not_applied")
	}
	if request.Expected != nil {
		if a.state.Identity.Matches(*request.Expected) != nil || a.s.conn != nil || a.state.Phase != "sleeping" {
			a.s.mu.Unlock()
			return apiFailure(r, "stale", "not_applied")
		}
	} else if a.s.conn != nil && a.ready {
		copy := a.state
		a.s.mu.Unlock()
		if copy.Settings != request.Settings {
			return apiFailure(r, "stale", "not_applied")
		}
		return apiSuccess(r, copy)
	}
	if err := a.begin(request.OperationID, "initialize", r.Params); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.cfg.Model, a.s.cfg.Effort, a.s.cfg.PermissionMode = request.Settings.Model, request.Settings.Effort, apiEngineMode(a.s.cfg.Kind, request.Settings.Mode)
	if err := a.s.start(); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	a.startConfirmation(request.OperationID, "initialize")
	a.s.publish()
	a.s.mu.Unlock()
	if !a.waitReady(ctx) {
		a.s.mu.Lock()
		if a.failure != nil {
			failure := a.failure
			a.s.mu.Unlock()
			return APIResponse{Protocol: 1, RequestID: r.RequestID, Error: failure}
		}
		copy := a.state
		a.s.mu.Unlock()
		return apiSuccess(r, copy)
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if err := a.finish(request.OperationID, "initialize", "confirmed", ""); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	return apiSuccess(r, a.state)
}

func (a *apiControl) prepare(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimePrepareRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	a.s.mu.Lock()
	if a.state.Identity.Matches(request.Expected) != nil || a.s.info.State != "idle" || a.s.conn == nil {
		a.s.mu.Unlock()
		return apiFailure(r, "busy", "not_applied")
	}
	if previous, ok := a.existing(request.OperationID, "prepare", r.Params); ok {
		a.s.mu.Unlock()
		if previous.Outcome == "confirmed" {
			a.s.mu.Lock()
			state := a.state
			a.s.mu.Unlock()
			return apiSuccess(r, state)
		}
		return apiFailure(r, "unknown", "unknown")
	}
	if err := a.begin(request.OperationID, "prepare", r.Params); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	if !a.ready || a.state.Settings != request.Settings {
		old := a.s.detach()
		a.s.mu.Unlock()
		if old.Close() != nil {
			return apiFailure(r, "unknown", "unknown")
		}
		a.s.mu.Lock()
		if a.s.conn != nil {
			a.s.mu.Unlock()
			return apiFailure(r, "stale", "unknown")
		}
		a.s.cfg.Model, a.s.cfg.Effort, a.s.cfg.PermissionMode = request.Settings.Model, request.Settings.Effort, apiEngineMode(a.s.cfg.Kind, request.Settings.Mode)
		if err := a.s.start(); err != nil {
			a.s.mu.Unlock()
			return apiFailure(r, "unknown", "unknown")
		}
		a.s.saveConfig()
		a.s.publish()
	}
	a.startConfirmation(request.OperationID, "prepare")
	a.s.publish()
	a.s.mu.Unlock()
	if !a.waitReady(ctx) {
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if err := a.finish(request.OperationID, "prepare", "confirmed", ""); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	return apiSuccess(r, a.state)
}

func (a *apiControl) send(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimeSendRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	a.s.mu.Lock()
	if a.state.Identity.Matches(request.Expected) != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "stale", "not_applied")
	}
	if previous, ok := a.existing(request.OperationID, "send", r.Params); ok {
		a.s.mu.Unlock()
		return apiSuccess(r, previous)
	}
	if !a.ready || a.s.info.State != "idle" || a.state.Settings != request.Settings || a.s.conn == nil {
		a.s.mu.Unlock()
		return apiFailure(r, "busy", "not_applied")
	}
	if err := a.begin(request.OperationID, "send", r.Params); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	turnID, _ := NewSessionID()
	a.state.Turn.ID = turnID
	a.s.info.State = "working"
	a.state.Turn.State = "working"
	conn := a.s.conn
	writer, ok := conn.(interface {
		SendConfirmed(context.Context, agent.Input) error
	})
	if !ok {
		a.s.mu.Unlock()
		return apiFailure(r, "unsupported", "not_applied")
	}
	a.s.publish()
	a.s.mu.Unlock()
	if err := writer.SendConfirmed(ctx, agent.Input{Text: request.Text}); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if a.s.conn != conn || a.state.Identity.Matches(request.Expected) != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	a.hasHistory = true
	a.s.began = true
	a.s.waiting = time.Now()
	if err := a.finish(request.OperationID, "send", "delivered", turnID); err != nil {
		a.s.publish()
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.publish()
	receipt, _ := a.read(request.OperationID)
	return apiSuccess(r, receipt)
}

func (a *apiControl) answer(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimeAnswerRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	a.s.mu.Lock()
	if a.state.Identity.Matches(request.Expected) != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "stale", "not_applied")
	}
	if previous, ok := a.existing(request.OperationID, "answer", r.Params); ok {
		a.s.mu.Unlock()
		return apiSuccess(r, previous)
	}
	pending, ok := a.pending[request.Request.ID]
	_, exists := a.s.pending[request.Request.ID]
	if !ok || !exists || pending.AgentIncarnation != request.Request.AgentIncarnation || a.s.conn == nil {
		a.s.mu.Unlock()
		return apiFailure(r, "stale", "not_applied")
	}
	if err := a.begin(request.OperationID, "answer", r.Params); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	o := op{Op: "deny", ID: pending.ID, Message: "Denied from the application."}
	if request.Decision == "allow" {
		o.Op = "allow"
		if pending.Kind == "question" {
			question := a.questions[pending.ID]
			mapped := map[string]string{}
			for i, q := range pending.Questions {
				if answer, ok := request.Answers[q.ID]; ok {
					key := question.Asks[i].Text
					mapped[key] = answer
				}
			}
			o.Input = AnswerInput(&question, mapped)
		} else {
			o.Input = jsontext.Value(pending.Input)
		}
	}
	conn := a.s.conn
	responder, supported := conn.(interface {
		AnswerConfirmed(context.Context, string, bool, jsontext.Value) error
	})
	if !supported {
		a.s.mu.Unlock()
		return apiFailure(r, "unsupported", "not_applied")
	}
	a.s.mu.Unlock()
	if err := responder.AnswerConfirmed(ctx, pending.ID, request.Decision == "allow", o.Input); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if a.s.conn != conn {
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.answered(pending.ID)
	a.resolve(pending.ID)
	a.s.afterAnswer()
	if err := a.finish(request.OperationID, "answer", "confirmed", pending.ID); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	a.s.publish()
	receipt, _ := a.read(request.OperationID)
	return apiSuccess(r, receipt)
}

func (a *apiControl) interrupt(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimeInterruptRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	a.s.mu.Lock()
	if a.state.Identity.Matches(request.Expected) != nil || a.state.Turn.ID != request.TurnID || a.s.info.State != "working" && a.s.info.State != "blocked" {
		a.s.mu.Unlock()
		return apiFailure(r, "stale", "not_applied")
	}
	if previous, ok := a.existing(request.OperationID, "interrupt", r.Params); ok {
		a.s.mu.Unlock()
		return apiSuccess(r, previous)
	}
	if err := a.begin(request.OperationID, "interrupt", r.Params); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	conn := a.s.conn
	a.s.mu.Unlock()
	interrupter, supported := conn.(interface{ InterruptConfirmed(context.Context) error })
	if !supported {
		return apiFailure(r, "unsupported", "not_applied")
	}
	if interrupter.InterruptConfirmed(ctx) != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	for {
		a.s.mu.Lock()
		if a.s.conn != conn {
			a.s.mu.Unlock()
			return apiFailure(r, "unknown", "unknown")
		}
		if a.s.info.State == "idle" && a.state.Turn.ID == request.TurnID {
			err := a.finish(request.OperationID, "interrupt", "confirmed", request.TurnID)
			receipt, _ := a.read(request.OperationID)
			a.s.mu.Unlock()
			if err != nil {
				return apiFailure(r, "unknown", "unknown")
			}
			return apiSuccess(r, receipt)
		}
		a.s.mu.Unlock()
		select {
		case <-ctx.Done():
			return apiFailure(r, "unknown", "unknown")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (a *apiControl) stop(ctx context.Context, r APIRequest) APIResponse {
	var request RuntimeStopRequest
	if json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return apiFailure(r, "invalid_input", "not_applied")
	}
	a.s.mu.Lock()
	if a.state.Identity.Matches(request.Expected) != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "stale", "not_applied")
	}
	if previous, ok := a.existing(request.OperationID, "stop", r.Params); ok {
		a.s.mu.Unlock()
		return apiSuccess(r, previous)
	}
	if err := a.begin(request.OperationID, "stop", r.Params); err != nil {
		a.s.mu.Unlock()
		return apiFailure(r, "unknown", "unknown")
	}
	conn := a.s.conn
	a.s.mu.Unlock()
	if conn != nil {
		gone := make(chan error, 1)
		go func() { gone <- conn.Close() }()
		select {
		case err := <-gone:
			if err != nil {
				return apiFailure(r, "unknown", "unknown")
			}
		case <-ctx.Done():
			return apiFailure(r, "unknown", "unknown")
		}
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if a.s.conn != nil && a.s.conn != conn {
		return apiFailure(r, "unknown", "unknown")
	}
	if err := a.finish(request.OperationID, "stop", "confirmed", ""); err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	receipt, _ := a.read(request.OperationID)
	a.s.detach()
	a.stopped = true
	a.s.info.State = "stopped"
	a.s.publish()
	return apiSuccess(r, receipt)
}

// APIEvents subscribes without waking or starting an unrelated host.
func APIEvents(ctx context.Context, r APIRequest, w io.Writer) error {
	var request RuntimeSubscribeRequest
	if !apiValidRequest(r) || r.Method != "subscribe" || json.Unmarshal(r.Params, &request) != nil || request.Validate() != nil {
		return WriteAPIResponse(w, apiFailure(r, "invalid_input", "not_applied"))
	}
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", apiSocket(request.Expected.HostID))
	if err != nil {
		return WriteAPIResponse(w, apiFailure(r, "unavailable", "not_applied"))
	}
	defer c.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			c.Close()
		case <-done:
		}
	}()
	b, _ := json.Marshal(r)
	if _, err = c.Write(append(b, '\n')); err != nil {
		return err
	}
	reader := bufio.NewReader(c)
	for {
		line, err := apiReadLine(reader)
		if err != nil {
			return err
		}
		if _, err = w.Write(line); err != nil {
			return err
		}
	}
}

func apiAtomic(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".api-write-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	return err
}
