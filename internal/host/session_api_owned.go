package host

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	apiOwnedMaxDirs     = 8192
	apiOwnedScanPerPage = 256
	apiOwnedConfigBytes = 64 << 10
)

// apiListOwned reads only this Rush home's public host records. It never
// starts, wakes, stops, or contacts a host, and it reports every host mapping
// so the caller can detect duplicate conversation IDs across pages.
func apiListOwned(ctx context.Context, r APIRequest, request RuntimeListOwnedRequest) APIResponse {
	page := RuntimeOwnedPage{Items: []RuntimeOwnedHost{}}
	f, err := os.Open(Root())
	if os.IsNotExist(err) {
		return apiSuccess(r, page)
	}
	if err != nil {
		return apiFailure(r, "permission", "not_applied")
	}
	defer f.Close()
	entries, err := f.ReadDir(apiOwnedMaxDirs + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return apiFailure(r, "unavailable", "not_applied")
	}
	if len(entries) > apiOwnedMaxDirs {
		return apiFailure(r, "overflow", "not_applied")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	start := sort.Search(len(entries), func(i int) bool { return entries[i].Name() > request.Cursor })
	scanned := 0
	for i := start; i < len(entries) && scanned < apiOwnedScanPerPage && len(page.Items) < request.Limit; i++ {
		if ctx.Err() != nil {
			return apiFailure(r, "cancelled", "not_applied")
		}
		e := entries[i]
		scanned++
		if e.IsDir() && runtimeToken(e.Name(), 128) {
			cfg, err := apiReadOwnedConfig(e.Name())
			if err == nil && cfg.PublicAPI && cfg.ID == e.Name() && runtimeUUID(cfg.SessionID) {
				owner := apiOwnership(cfg.Meta)
				if owner.Validate() == nil && owner.Installation == request.Installation {
					page.Items = append(page.Items, apiOwnedHost(cfg, owner))
				}
			}
		}
		if i+1 < len(entries) && (scanned == apiOwnedScanPerPage || len(page.Items) == request.Limit) {
			page.NextCursor = e.Name()
		}
	}
	return apiSuccess(r, page)
}

func apiReadOwnedConfig(id string) (Config, error) {
	var cfg Config
	path := filepath.Join(dir(id), "config.json")
	st, err := os.Lstat(path)
	if err != nil {
		return cfg, err
	}
	if !st.Mode().IsRegular() {
		return cfg, errors.New("invalid public host config")
	}
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, apiOwnedConfigBytes+1))
	if err != nil {
		return cfg, err
	}
	if len(b) > apiOwnedConfigBytes || json.Unmarshal(b, &cfg) != nil {
		return cfg, errors.New("invalid public host config")
	}
	return cfg, nil
}

func apiOwnership(m map[string]string) RuntimeOwnership {
	return RuntimeOwnership{Version: m["amtana_metadata_version"], Installation: m["amtana_installation"], Chat: m["amtana_chat"], Operation: m["amtana_operation"]}
}

func apiOwnedHost(cfg Config, owner RuntimeOwnership) RuntimeOwnedHost {
	item := RuntimeOwnedHost{ConversationID: cfg.SessionID, HostID: cfg.ID, Metadata: owner, Status: "unknown"}
	state, err := apiReadState(cfg.ID)
	if err != nil {
		if !os.IsNotExist(err) {
			item.Status = "conflict"
		}
		return item
	}
	if state.Identity.ConversationID != cfg.SessionID || state.Identity.HostID != cfg.ID || state.Identity.Metadata != owner {
		item.Status = "conflict"
		return item
	}
	info, err := ReadInfo(cfg.ID)
	if err != nil || info.ID != cfg.ID || info.SessionID != cfg.SessionID {
		return item
	}
	item.State = &state
	if info.State == "stopped" || state.Phase == "stopped" || !Alive(info.HostPID) {
		item.Status = "stopped"
	} else {
		item.Status = "found"
	}
	return item
}
