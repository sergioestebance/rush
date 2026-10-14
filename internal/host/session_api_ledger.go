package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// The operation ledger stores identity, bounded fingerprints and outcomes only.
// It never stores prompts. Direct lookup keeps heap independent of journal age.
type apiLedgerEntry struct {
	Receipt     RuntimeReceipt `json:"receipt"`
	Fingerprint string         `json:"fingerprint"`
}

func apiLedgerPath(id, operation string) string {
	return filepath.Join(dir(id), "operations", operation+".json")
}

func apiFingerprint(params []byte) string {
	sum := sha256.Sum256(params)
	return hex.EncodeToString(sum[:])
}

func apiReadLedger(id, operation string) (apiLedgerEntry, error) {
	var entry apiLedgerEntry
	if !runtimeUUID(operation) {
		return entry, errors.New("invalid operation")
	}
	b, err := apiReadFile(apiLedgerPath(id, operation), 16<<10)
	if err != nil {
		return entry, err
	}
	if json.Unmarshal(b, &entry) != nil {
		return entry, errors.New("invalid ledger")
	}
	return entry, nil
}

// apiReplaysInitialize reports whether params repeat the initialization this
// host recorded under operation. A replay keeps the folder the host followed
// its agent to: the caller only knew the folder it first asked for.
func apiReplaysInitialize(id, operation string, params []byte) bool {
	entry, err := apiReadLedger(id, operation)
	return err == nil && entry.Receipt.Kind == "initialize" && entry.Fingerprint == apiFingerprint(params)
}

func (a *apiControl) read(operation string) (RuntimeReceipt, error) {
	entry, err := apiReadLedger(a.s.cfg.ID, operation)
	return entry.Receipt, err
}

func (a *apiControl) existing(operation, kind string, params []byte) (RuntimeReceipt, bool) {
	entry, err := apiReadLedger(a.s.cfg.ID, operation)
	if os.IsNotExist(err) {
		return RuntimeReceipt{}, false
	}
	if err != nil {
		return RuntimeReceipt{OperationID: operation, Kind: kind, Outcome: "unknown", Identity: a.state.Identity}, true
	}
	if entry.Fingerprint != apiFingerprint(params) || entry.Receipt.Kind != kind {
		return RuntimeReceipt{OperationID: operation, Kind: kind, Outcome: "unknown", Identity: a.state.Identity}, true
	}
	return entry.Receipt, true
}

func (a *apiControl) begin(operation, kind string, params []byte) error {
	if !runtimeUUID(operation) {
		return errors.New("invalid operation")
	}
	if previous, err := apiReadLedger(a.s.cfg.ID, operation); err == nil {
		if previous.Fingerprint != apiFingerprint(params) || previous.Receipt.Kind != kind {
			return errors.New("operation conflict")
		}
		if previous.Receipt.Identity.HostIncarnation == a.state.Identity.HostIncarnation {
			return nil
		}
		// Initialization of a confirmed-dead host can use the stored creation
		// identity. A previous send/answer is never repeated on replacement.
		if kind != "initialize" {
			return errors.New("previous incarnation operation")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir(a.s.cfg.ID), "operations"), 0700); err != nil {
		return err
	}
	entry := apiLedgerEntry{Receipt: RuntimeReceipt{OperationID: operation, Kind: kind, Outcome: "unknown", Identity: a.state.Identity}, Fingerprint: apiFingerprint(params)}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return apiAtomic(apiLedgerPath(a.s.cfg.ID, operation), b)
}

func (a *apiControl) finish(operation, kind, outcome, related string) error {
	entry, err := apiReadLedger(a.s.cfg.ID, operation)
	if err != nil {
		return err
	}
	if kind != "stop" {
		entry.Receipt.Identity = a.state.Identity
	}
	entry.Receipt.Kind, entry.Receipt.Outcome = kind, outcome
	switch kind {
	case "send", "interrupt":
		entry.Receipt.TurnID = related
	case "answer":
		entry.Receipt.RequestID = related
	}
	if outcome == "delivered" {
		entry.Receipt.DeliveredAt = time.Now()
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err = apiAtomic(apiLedgerPath(a.s.cfg.ID, operation), b); err != nil {
		return err
	}
	if kind == "send" && outcome == "delivered" {
		receipt := entry.Receipt
		a.state.LastConfirmedDelivery = &receipt
	}
	a.sequence++
	a.state.Cursor = RuntimeCursor{HostIncarnation: a.state.Identity.HostIncarnation, Sequence: a.sequence}
	a.emit(RuntimeEvent{Type: "receipt", Identity: a.state.Identity, Cursor: a.state.Cursor, Receipt: &entry.Receipt})
	return nil
}

func (a *apiControl) lookupReceipt(r APIRequest, operation string) APIResponse {
	receipt, err := a.read(operation)
	if os.IsNotExist(err) {
		receipt = RuntimeReceipt{OperationID: operation, Kind: "unknown", Outcome: "not_known", Identity: a.state.Identity}
		return apiSuccess(r, receipt)
	}
	if err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	return apiSuccess(r, receipt)
}

func apiReadReceipt(ctx context.Context, r APIRequest, lookup RuntimeReceiptLookup) APIResponse {
	id := lookup.Expected.HostID
	if id == "" {
		id = ShortID(lookup.Expected.ConversationID)
	}
	state, err := apiReadState(id)
	if err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	if !apiMatches(state, lookup.Expected) {
		return apiFailure(r, "conflict", "not_applied")
	}
	entry, err := apiReadLedger(id, lookup.OperationID)
	if os.IsNotExist(err) {
		return apiSuccess(r, RuntimeReceipt{OperationID: lookup.OperationID, Identity: state.Identity, Kind: "unknown", Outcome: "not_known"})
	}
	if err != nil {
		return apiFailure(r, "unknown", "unknown")
	}
	return apiSuccess(r, entry.Receipt)
}
