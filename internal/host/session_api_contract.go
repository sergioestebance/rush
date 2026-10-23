// Version-1 executable protocol values are owned by the host transport.
package host

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/0xdeafcafe/rush/internal/agent"
)

// RuntimeOwnership tags provenance. They grant no client authorization.
type RuntimeOwnership struct {
	Version      string `json:"amtana_metadata_version"`
	Installation string `json:"amtana_installation"`
	Chat         string `json:"amtana_chat"`
	Operation    string `json:"amtana_operation"`
}

// Validate refuses incomplete or unsupported ownership metadata.
func (o RuntimeOwnership) Validate() error {
	if o.Version != "1" || !runtimeUUID(o.Installation) || !runtimeUUID(o.Operation) || !runtimeToken(o.Chat, 64) {
		return errors.New("runtime: invalid ownership metadata")
	}
	return nil
}

// RuntimeIdentity separates a conversation from each host and agent process.
type RuntimeIdentity struct {
	ConversationID   string           `json:"conversationId"`
	HostID           string           `json:"hostId"`
	HostIncarnation  string           `json:"hostIncarnation"`
	AgentIncarnation string           `json:"agentIncarnation,omitempty"`
	Metadata         RuntimeOwnership `json:"metadata"`
}

// Validate requires a complete host identity and optionally a live agent identity.
func (i RuntimeIdentity) Validate(requireAgent bool) error {
	if err := i.Metadata.Validate(); err != nil {
		return err
	}
	if !runtimeUUID(i.ConversationID) || !runtimeToken(i.HostID, 128) || !runtimeUUID(i.HostIncarnation) {
		return errors.New("runtime: invalid host identity")
	}
	if requireAgent && i.AgentIncarnation == "" || i.AgentIncarnation != "" && !runtimeUUID(i.AgentIncarnation) {
		return errors.New("runtime: invalid agent incarnation")
	}
	return nil
}

// Matches checks a control response against the complete expected host. An empty
// expected agent permits inspection of a sleeping host, never an agent control.
func (i RuntimeIdentity) Matches(expected RuntimeIdentity) error {
	if err := expected.Validate(false); err != nil {
		return err
	}
	if err := i.Validate(expected.AgentIncarnation != ""); err != nil {
		return err
	}
	if i.ConversationID != expected.ConversationID || i.HostID != expected.HostID ||
		i.HostIncarnation != expected.HostIncarnation || i.Metadata != expected.Metadata ||
		expected.AgentIncarnation != "" && i.AgentIncarnation != expected.AgentIncarnation {
		return errors.New("runtime: identity or incarnation changed")
	}
	return nil
}

// RuntimeSettings gives transport explicit names without changing legacy settings.
type RuntimeSettings struct {
	Model  string `json:"model"`
	Effort string `json:"effort"`
	Mode   string `json:"mode"`
}

// Validate preserves the app's closed settings lists at the execution boundary.
// Provider aliases are translated by the adapter, never supplied as extra flags.
func (s RuntimeSettings) Validate() error {
	switch s.Model {
	case "fable", "opus", "sonnet", "haiku":
	default:
		return errors.New("runtime: unknown model")
	}
	switch s.Effort {
	case "low", "medium", "high", "xhigh", "max":
	default:
		return errors.New("runtime: unknown effort")
	}
	switch s.Mode {
	case "ask", "auto":
	default:
		return errors.New("runtime: unknown approval mode")
	}
	return nil
}

type RuntimeBuild struct {
	Protocol int    `json:"protocol"`
	Version  string `json:"version"`
	Revision string `json:"revision"`
}

type RuntimeProvider struct {
	Kind     string   `json:"kind"`
	Features []string `json:"features"`
}

type RuntimeCapabilities struct {
	Protocol  int               `json:"protocol"`
	Build     RuntimeBuild      `json:"build"`
	Features  []string          `json:"features"`
	Providers []RuntimeProvider `json:"providers"`
}

type RuntimeTurn struct {
	State string `json:"state"`
	ID    string `json:"id,omitempty"`
}

type RuntimeCursor struct {
	HostIncarnation string `json:"hostIncarnation"`
	Sequence        uint64 `json:"sequence"`
}

// RuntimeState is observed execution state, never an echo of queued app messages.
//
// Phase is one of host_starting, agent_starting, ready, sleeping, failed and
// stopped. PhaseSince is when it was entered. Deadline is set while a phase
// is bounded (agent_starting ends in ready or failed by then); a phase past
// its deadline is the host's fault, never the owner's to wait out. Failure
// says why the host is failed; a new initialize or prepare clears it.
type RuntimeState struct {
	Identity          RuntimeIdentity `json:"identity"`
	Build             RuntimeBuild    `json:"build"`
	Phase             string          `json:"phase"`
	PhaseSince        time.Time       `json:"phaseSince,omitzero"`
	Deadline          time.Time       `json:"deadline,omitzero"`
	Failure           *RuntimeError   `json:"failure,omitempty"`
	Turn              RuntimeTurn     `json:"turn"`
	Cwd               string          `json:"cwd"`
	Settings          RuntimeSettings `json:"settings"`
	SettingsConfirmed bool            `json:"settingsConfirmed"`
	Cursor            RuntimeCursor   `json:"cursor"`
	// LastConfirmedDelivery is historical evidence, not proof that a later send
	// was delivered. Automatic continuation requires a delivered receipt matching
	// this state's complete identity and current turn ID. Unknown state or an
	// active turn without that receipt freezes recovery. A vanished memory queue
	// cannot establish any message outcome after daemon restart.
	LastConfirmedDelivery *RuntimeReceipt `json:"lastConfirmedDelivery,omitempty"`
}

// ValidateReady rejects configuration echoes and another conversation's response.
// The adapter normalizes supported engine aliases before returning these settings.
func (s RuntimeState) ValidateReady(want RuntimeInitializeRequest) error {
	if err := want.Validate(); err != nil {
		return err
	}
	if err := s.Identity.Validate(true); err != nil {
		return err
	}
	if s.Phase != "ready" || !s.SettingsConfirmed || s.Settings != want.Settings || s.Cwd != want.Cwd ||
		s.Identity.ConversationID != want.ConversationID || s.Identity.Metadata != want.Metadata {
		return errors.New("runtime: engine readiness is unconfirmed or mismatched")
	}
	if want.Expected != nil {
		return s.Identity.Matches(*want.Expected)
	}
	return nil
}

// RuntimeInitializeRequest contains no prompt: readiness precedes delivery.
type RuntimeInitializeRequest struct {
	OperationID    string           `json:"operationId"`
	ConversationID string           `json:"conversationId"`
	Metadata       RuntimeOwnership `json:"metadata"`
	// Expected guards a wake of the same sleeping host. Nil initializes a new
	// host, optionally resuming provider history; these are distinct operations.
	Expected     *RuntimeIdentity `json:"expected,omitempty"`
	Cwd          string           `json:"cwd"`
	Provider     string           `json:"provider"`
	EngineBinary string           `json:"engineBinary,omitempty"`
	Resume       bool             `json:"resume"`
	// ForkFrom resumes a copy of this source into ConversationID. It neither
	// resumes the target's history nor wakes an existing host.
	ForkFrom string          `json:"forkFrom,omitempty"`
	Settings RuntimeSettings `json:"settings"`
}

// Validate checks immutable initialization identity before a transport call.
func (r RuntimeInitializeRequest) Validate() error {
	if err := r.Metadata.Validate(); err != nil {
		return err
	}
	if !runtimeUUID(r.ConversationID) || !runtimeUUID(r.OperationID) || !filepath.IsAbs(r.Cwd) ||
		strings.IndexByte(r.Cwd, 0) >= 0 ||
		r.EngineBinary != "" && (!filepath.IsAbs(r.EngineBinary) || strings.IndexByte(r.EngineBinary, 0) >= 0) {
		return errors.New("runtime: invalid initialization request")
	}
	if _, ok := agent.As[agent.SessionAPIProvider](agent.Kind(r.Provider)); !ok {
		return errors.New("runtime: unsupported session API provider")
	}
	if r.Expected == nil {
		if r.OperationID != r.Metadata.Operation {
			return errors.New("runtime: initialization does not name its recorded operation")
		}
	} else {
		if err := r.Expected.Validate(false); err != nil {
			return err
		}
		if r.Resume || r.OperationID == r.Metadata.Operation || r.Expected.AgentIncarnation != "" ||
			r.Expected.ConversationID != r.ConversationID || r.Expected.Metadata != r.Metadata {
			return errors.New("runtime: invalid same-host wake")
		}
	}
	if r.ForkFrom != "" && (!runtimeUUID(r.ForkFrom) || r.ForkFrom == r.ConversationID || r.Resume || r.Expected != nil) {
		return errors.New("runtime: invalid source or target for a fork")
	}
	return r.Settings.Validate()
}

// RuntimeLookup supports recovery before the returned host mapping was recorded.
type RuntimeLookup struct {
	ConversationID string           `json:"conversationId"`
	HostID         string           `json:"hostId,omitempty"`
	Metadata       RuntimeOwnership `json:"metadata"`
}

// Validate permits an unknown host ID during reconciliation, never a short
// conversation ID or incomplete ownership metadata.
func (r RuntimeLookup) Validate() error {
	if err := r.Metadata.Validate(); err != nil {
		return err
	}
	if !runtimeUUID(r.ConversationID) || r.HostID != "" && !runtimeToken(r.HostID, 128) {
		return errors.New("runtime: invalid conversation lookup")
	}
	return nil
}

type RuntimeInspection struct {
	Status  string         `json:"status"`
	State   *RuntimeState  `json:"state,omitempty"`
	Matches []RuntimeState `json:"matches,omitempty"`
}

// RuntimeListOwnedRequest pages only public hosts tagged by one installation.
// Metadata describes provenance; it is not an authorization credential.
type RuntimeListOwnedRequest struct {
	Installation string `json:"installation"`
	Cursor       string `json:"cursor,omitempty"`
	Limit        int    `json:"limit"`
}

func (r RuntimeListOwnedRequest) Validate() error {
	if !runtimeUUID(r.Installation) || r.Cursor != "" && !runtimeToken(r.Cursor, 128) || r.Limit < 1 || r.Limit > 100 {
		return errors.New("runtime: invalid owned-host listing request")
	}
	return nil
}

// RuntimeOwnedHost includes a config-only host whose state file is missing.
// Such an entry is diagnostic and never grants permission to resume or stop.
type RuntimeOwnedHost struct {
	ConversationID string           `json:"conversationId"`
	HostID         string           `json:"hostId"`
	Metadata       RuntimeOwnership `json:"metadata"`
	Status         string           `json:"status"`
	State          *RuntimeState    `json:"state,omitempty"`
}

type RuntimeOwnedPage struct {
	Items      []RuntimeOwnedHost `json:"items"`
	NextCursor string             `json:"nextCursor,omitempty"`
}

func (p RuntimeOwnedPage) Validate(request RuntimeListOwnedRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if p.Items == nil || len(p.Items) > request.Limit ||
		p.NextCursor != "" && (!runtimeToken(p.NextCursor, 128) || p.NextCursor <= request.Cursor) {
		return errors.New("runtime: invalid owned-host page")
	}
	seen := make(map[string]bool, len(p.Items))
	last := request.Cursor
	for _, item := range p.Items {
		if !runtimeUUID(item.ConversationID) || !runtimeToken(item.HostID, 128) || item.Metadata.Validate() != nil ||
			item.Metadata.Installation != request.Installation || seen[item.HostID] || item.HostID <= last {
			return errors.New("runtime: invalid owned-host entry")
		}
		seen[item.HostID] = true
		last = item.HostID
		switch item.Status {
		case "found", "stopped", "unknown", "conflict":
		default:
			return errors.New("runtime: invalid owned-host status")
		}
		if (item.Status == "found" || item.Status == "stopped") != (item.State != nil) {
			return errors.New("runtime: owned-host state availability contradicts status")
		}
		if item.State != nil && (item.Status == "unknown" || item.Status == "conflict" ||
			item.State.Identity.ConversationID != item.ConversationID || item.State.Identity.HostID != item.HostID ||
			item.State.Identity.Metadata != item.Metadata || item.State.Identity.Validate(false) != nil) {
			return errors.New("runtime: owned-host state is mismatched")
		}
	}
	if p.NextCursor != "" && p.NextCursor < last {
		return errors.New("runtime: owned-host cursor precedes its page")
	}
	return nil
}

type RuntimePrepareRequest struct {
	OperationID string          `json:"operationId"`
	Expected    RuntimeIdentity `json:"expected"`
	Settings    RuntimeSettings `json:"settings"`
}

// Validate requires a named live agent before settings for the next turn change.
func (r RuntimePrepareRequest) Validate() error {
	if err := r.Expected.Validate(true); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) {
		return errors.New("runtime: invalid prepare operation")
	}
	return r.Settings.Validate()
}

// RuntimeImage names one image file the host reads before it submits the
// message. Element N of a send's images is the one its text names [Image #N].
type RuntimeImage struct {
	Path      string `json:"path"`
	MediaType string `json:"mediaType"`
}

// Validate checks the name only; apiReadImages checks the file itself.
func (i RuntimeImage) Validate() error {
	if i.MediaType != "image/jpeg" && i.MediaType != "image/png" {
		return errors.New("runtime: unsupported image media type")
	}
	if !filepath.IsAbs(i.Path) || filepath.Clean(i.Path) != i.Path || strings.IndexByte(i.Path, 0) >= 0 {
		return errors.New("runtime: invalid image path")
	}
	return nil
}

type RuntimeSendRequest struct {
	OperationID      string          `json:"operationId"`
	Expected         RuntimeIdentity `json:"expected"`
	RequireTurnState string          `json:"requireTurnState"`
	Settings         RuntimeSettings `json:"settings"`
	Text             string          `json:"text"`
	Images           []RuntimeImage  `json:"images,omitempty"`
}

// Validate refuses a send that could reach a different writer or an active turn.
// Text is never blank, even when it only names its images.
func (r RuntimeSendRequest) Validate() error {
	if err := r.Expected.Validate(true); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) || r.RequireTurnState != "idle" || strings.TrimSpace(r.Text) == "" || !utf8.ValidString(r.Text) {
		return errors.New("runtime: invalid conditional send")
	}
	if len(r.Images) > APIImageCount {
		return errors.New("runtime: too many images")
	}
	for _, image := range r.Images {
		if err := image.Validate(); err != nil {
			return err
		}
	}
	return r.Settings.Validate()
}

type RuntimeReceiptLookup struct {
	OperationID string        `json:"operationId"`
	Expected    RuntimeLookup `json:"expected"`
}

// Validate names one mutation without requiring its previous agent to survive.
func (r RuntimeReceiptLookup) Validate() error {
	if err := r.Expected.Validate(); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) {
		return errors.New("runtime: invalid receipt operation")
	}
	return nil
}

// RuntimeReceipt acknowledges one operation. Unknown remains distinct from refusal.
type RuntimeReceipt struct {
	OperationID string          `json:"operationId"`
	Identity    RuntimeIdentity `json:"identity"`
	Kind        string          `json:"kind"`
	Outcome     string          `json:"outcome"`
	TurnID      string          `json:"turnId,omitempty"`
	RequestID   string          `json:"requestId,omitempty"`
	DeliveredAt time.Time       `json:"deliveredAt,omitzero"`
}

// Validate refuses receipts from another operation, host or agent incarnation.
// Validation preserves unknown/not-known outcomes; it never converts them to sent.
// A delete receipt names the conversation, host and owner only: the host it
// removed may never have recorded an incarnation.
func (r RuntimeReceipt) Validate(expected RuntimeIdentity, operation, kind string) error {
	if !runtimeUUID(operation) || r.OperationID != operation || r.Kind != kind {
		return errors.New("runtime: receipt does not name the operation")
	}
	if kind == "delete" {
		if r.Identity.ConversationID != expected.ConversationID || r.Identity.HostID != expected.HostID || r.Identity.Metadata != expected.Metadata {
			return errors.New("runtime: delete receipt names another conversation")
		}
	} else if err := r.Identity.Matches(expected); err != nil {
		return err
	}
	switch r.Outcome {
	case "delivered":
		if kind != "send" || expected.AgentIncarnation == "" || !runtimeUUID(r.TurnID) {
			return errors.New("runtime: incomplete delivery receipt")
		}
	case "confirmed":
		switch kind {
		case "initialize", "prepare":
			if err := r.Identity.Validate(true); err != nil {
				return errors.New("runtime: incomplete initialization or settings receipt")
			}
		case "answer":
			if expected.AgentIncarnation == "" || !runtimeToken(r.RequestID, 256) {
				return errors.New("runtime: incomplete answer receipt")
			}
		case "interrupt":
			if expected.AgentIncarnation == "" || !runtimeUUID(r.TurnID) {
				return errors.New("runtime: incomplete interrupt receipt")
			}
		case "stop", "delete":
		default:
			return errors.New("runtime: invalid confirmed receipt kind")
		}
	case "rejected", "unknown", "not_known", "expired":
	default:
		return errors.New("runtime: invalid receipt outcome")
	}
	return nil
}

type RuntimeSubscribeRequest struct {
	Expected RuntimeIdentity `json:"expected"`
	After    *RuntimeCursor  `json:"after,omitempty"`
}

// Validate refuses a cursor from another host rather than merging its requests.
func (r RuntimeSubscribeRequest) Validate() error {
	if err := r.Expected.Validate(false); err != nil {
		return err
	}
	if r.After != nil && r.After.HostIncarnation != r.Expected.HostIncarnation {
		return errors.New("runtime: subscription cursor names another host")
	}
	return nil
}

type RuntimeChoice struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

type RuntimeQuestion struct {
	ID       string          `json:"id"`
	Text     string          `json:"text"`
	Multiple bool            `json:"multiple"`
	Choices  []RuntimeChoice `json:"choices"`
}

type RuntimePendingRequest struct {
	ID               string            `json:"id"`
	AgentIncarnation string            `json:"agentIncarnation"`
	Kind             string            `json:"kind"`
	Tool             string            `json:"tool,omitempty"`
	CallID           string            `json:"callId,omitempty"`
	Input            json.RawMessage   `json:"input,omitempty"`
	Reason           string            `json:"reason,omitempty"`
	Questions        []RuntimeQuestion `json:"questions,omitempty"`
}

type RuntimeSnapshot struct {
	State   RuntimeState            `json:"state"`
	Cursor  RuntimeCursor           `json:"cursor"`
	Pending []RuntimePendingRequest `json:"pending"`
}

// Validate requires a complete pending set from the expected host/agent. Resource
// bounds and event ordering are checked by the transport and consuming session.
func (s RuntimeSnapshot) Validate(expected RuntimeIdentity) error {
	if err := s.State.Identity.Matches(expected); err != nil {
		return err
	}
	if s.Cursor.HostIncarnation != s.State.Identity.HostIncarnation || s.State.Cursor != s.Cursor {
		return errors.New("runtime: snapshot state and pending cursor differ")
	}
	if s.Pending == nil {
		return errors.New("runtime: snapshot has no complete pending set")
	}
	seen := make(map[string]bool, len(s.Pending))
	for _, p := range s.Pending {
		if !runtimeToken(p.ID, 256) || seen[p.ID] || p.AgentIncarnation == "" || p.AgentIncarnation != s.State.Identity.AgentIncarnation {
			return errors.New("runtime: invalid or stale pending request")
		}
		seen[p.ID] = true
		switch p.Kind {
		case "approval":
			if p.Tool == "" || len(p.Input) == 0 || !json.Valid(p.Input) {
				return errors.New("runtime: incomplete approval request")
			}
		case "question":
			if len(p.Questions) == 0 {
				return errors.New("runtime: incomplete question request")
			}
			questions := make(map[string]bool, len(p.Questions))
			for _, q := range p.Questions {
				if !runtimeToken(q.ID, 256) || q.Text == "" || questions[q.ID] {
					return errors.New("runtime: invalid question identity")
				}
				questions[q.ID] = true
				choices := make(map[string]bool, len(q.Choices))
				for _, c := range q.Choices {
					if !runtimeToken(c.ID, 256) || c.Label == "" || choices[c.ID] {
						return errors.New("runtime: invalid question choice")
					}
					choices[c.ID] = true
				}
			}
		default:
			return errors.New("runtime: unsupported pending request")
		}
	}
	return nil
}

type RuntimeRequestIdentity struct {
	ID               string `json:"id"`
	AgentIncarnation string `json:"agentIncarnation"`
}

type RuntimeAnswerRequest struct {
	OperationID string                 `json:"operationId"`
	Expected    RuntimeIdentity        `json:"expected"`
	Request     RuntimeRequestIdentity `json:"request"`
	Decision    string                 `json:"decision"`
	Answers     map[string]string      `json:"answers,omitempty"`
}

// Validate binds an answer to one current request before any transport mutation.
func (r RuntimeAnswerRequest) Validate() error {
	if err := r.Expected.Validate(true); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) || !runtimeToken(r.Request.ID, 256) ||
		r.Request.AgentIncarnation != r.Expected.AgentIncarnation || r.Decision != "allow" && r.Decision != "deny" {
		return errors.New("runtime: invalid or stale answer")
	}
	return nil
}

type RuntimeInterruptRequest struct {
	OperationID string          `json:"operationId"`
	Expected    RuntimeIdentity `json:"expected"`
	TurnID      string          `json:"turnId"`
}

// Validate prevents a delayed interrupt from cancelling an unnamed later turn.
func (r RuntimeInterruptRequest) Validate() error {
	if err := r.Expected.Validate(true); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) || !runtimeUUID(r.TurnID) {
		return errors.New("runtime: invalid interrupt operation or turn")
	}
	return nil
}

type RuntimeStopRequest struct {
	OperationID string          `json:"operationId"`
	Expected    RuntimeIdentity `json:"expected"`
}

// Validate scopes stop to a host incarnation; a sleeping agent is not required.
func (r RuntimeStopRequest) Validate() error {
	if err := r.Expected.Validate(false); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) {
		return errors.New("runtime: invalid stop operation")
	}
	return nil
}

// RuntimeDeleteRequest removes a conversation's host for good: its process,
// its folder and its scratch. It names the host as inspect does, since the
// host may be dead, failed or never recorded a state; it never needs a live
// incarnation. Absence is the proof: a repeat is confirmed again.
type RuntimeDeleteRequest struct {
	OperationID string        `json:"operationId"`
	Expected    RuntimeLookup `json:"expected"`
}

// Validate names one removal of one owned conversation.
func (r RuntimeDeleteRequest) Validate() error {
	if err := r.Expected.Validate(); err != nil {
		return err
	}
	if !runtimeUUID(r.OperationID) {
		return errors.New("runtime: invalid delete operation")
	}
	return nil
}

// RuntimeEvent carries control state. Diary pages remain the transcript source.
type RuntimeEvent struct {
	Type     string                  `json:"event"`
	Identity RuntimeIdentity         `json:"identity"`
	Cursor   RuntimeCursor           `json:"cursor"`
	Snapshot *RuntimeSnapshot        `json:"snapshot,omitempty"`
	State    *RuntimeState           `json:"state,omitempty"`
	Request  *RuntimePendingRequest  `json:"request,omitempty"`
	Resolved *RuntimeRequestIdentity `json:"resolved,omitempty"`
	Receipt  *RuntimeReceipt         `json:"receipt,omitempty"`
	Error    *RuntimeError           `json:"error,omitempty"`
}

// RuntimeError distinguishes an uncertain mutation from a definite refusal. Its
// Message is a sanitized reason; transport must never supply raw process output.
type RuntimeError struct {
	Code    string `json:"code"`
	Outcome string `json:"outcome"`
	Retry   string `json:"retry"`
	Message string `json:"message"`
}

func (e *RuntimeError) Error() string { return e.Message }

// ConversationID is whether s is fit to name a conversation: a UUID.
func ConversationID(s string) bool { return runtimeUUID(s) }

func runtimeUUID(s string) bool {
	if len(s) != 36 || s == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if s[i] != '-' {
				return false
			}
		} else if !strings.ContainsRune("0123456789abcdefABCDEF", rune(s[i])) {
			return false
		}
	}
	return true
}

func runtimeToken(s string, max int) bool {
	if s == "" || len(s) > max || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
