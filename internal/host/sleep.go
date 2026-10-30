package host

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Ensure wakes a saved host for an explicit action. Viewing/Dial never wakes a
// host. Concurrent sends share a startup lock, so only one process is spawned.
func Ensure(id string) error { return ensure(id, "") }

// ensure is Ensure, starting the host with prompt as its first message.
func ensure(id, prompt string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\\x00") {
		return errors.New("invalid session ID")
	}
	if info, err := readInfoFile(id); err == nil && info.IsRemote() {
		if alive(info.HostPID) {
			return nil // the machine it's on wakes it, on a send
		}
		return errors.New(id + " is on " + info.Remote + ", and rush remote attach " + info.Remote + " isn't running")
	}
	unlock, err := lockWake(id)
	if err != nil {
		return err
	}
	defer unlock()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := ReadInfo(id)
		if err != nil || !alive(info.HostPID) {
			break
		}
		if !info.Sleeping {
			if c, err := net.DialTimeout("unix", SockPath(id), 250*time.Millisecond); err == nil {
				c.Close()
				return nil
			}
		}
		if time.Now().After(deadline) {
			return errors.New("session host did not become ready or finish exiting")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Read again under the lock: another start or model change may have updated it.
	cfg, err := ReadConfig(id)
	if err != nil {
		return err
	}
	cfg.Prompt, cfg.Resume = prompt, cfg.Resume || prompt != ""
	cfg.Images = nil
	cfg.PromptExchange = nil
	cfg.Owner = 0
	_, err = Spawn(cfg)
	return err
}

// wakeWait is how long a wake, or a restart for MCP servers, waits for
// another to let go of the session's wake lock.
const wakeWait = 15 * time.Second

// lockWake takes session id's wake lock, which one wake or restart holds
// at a time so only one host starts, waiting up to wakeWait for it.
func lockWake(id string) (unlock func(), err error) {
	lock, err := os.OpenFile(filepath.Join(dir(id), "wake.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wakeWait)
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// Closing the file lets go of the lock.
			return func() { lock.Close() }, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			lock.Close()
			return nil, err
		}
		if time.Now().After(deadline) {
			lock.Close()
			return nil, errors.New("timed out waiting for session startup")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A clean sleep has no pending work, but its observed choices and cumulative
// counters still belong to this conversation. New/forked sessions start fresh.
func (s *server) restoreSleepingInfo(old Info) {
	if !old.Sleeping || !s.cfg.Resume || old.SessionID != s.cfg.SessionID || old.Kind != s.info.Kind {
		return
	}
	s.info.PermissionModes = old.PermissionModes
	if s.info.PermissionMode == "" {
		s.info.PermissionMode = old.PermissionMode
	}
	s.info.CostUSD = old.CostUSD
	s.info.ContextTokens = old.ContextTokens
	s.info.CacheWarm = old.CacheWarm
	s.info.Home, s.info.HomeAt = old.Home, old.HomeAt
	s.info.Billing = old.Billing
	s.info.QueueHeld = old.QueueHeld
	s.info.QueueSeparate = old.QueueSeparate
	s.info.Detail = old.Detail
	if s.info.Model == "" {
		s.info.Model = old.Model
	}
	if s.info.Effort == "" {
		s.info.Effort = old.Effort
	}
}
