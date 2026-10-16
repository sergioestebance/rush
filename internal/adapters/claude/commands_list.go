package claude

import (
	"strings"

	"github.com/0xdeafcafe/rush/internal/adapters/claude/headless"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// OffCommands are Claude Code's that do nothing in rush, and why: its own
// terminal's, the odds and ends, and the ones rush hasn't done yet.
var OffCommands = func() map[string]string {
	out := map[string]string{}
	for _, n := range strings.Fields("theme color tui scroll-speed focus brief terminal-setup voice keybindings ide chrome exit quit vim") {
		out[n] = "it's for Claude Code's own terminal"
	}
	for _, n := range strings.Fields("release-notes feedback bug share update install import powerup wellbeing breaks break-reminder downtime stickers radio heapdump passes agents plugin-types workflow-launch-exec") {
		out[n] = "rush leaves it out"
	}
	out["loops"] = "Claude Code has it switched off; ask Claude to /loop instead"
	return out
}()

// rushOwn are commands rush does itself, in its own way, rather than
// passing them to Claude Code: its list leaves them out.
var rushOwn = map[string]bool{"clear": true, "model": true, "resume": true, "rewind": true}

// listCommands keeps list as the session's commands and passes on what of
// it a client can use: not what OffCommands or rushOwn name, nor what init
// said only Claude Code's terminal does.
func (c *conn) listCommands(list []headless.Command) {
	c.mu.Lock()
	c.commands = list
	off := c.terminal
	c.mu.Unlock()
	var out event.Commands
	for _, k := range list {
		if OffCommands[k.Name] != "" || rushOwn[k.Name] || off[k.Name] {
			continue
		}
		out.List = append(out.List, event.Command(k))
	}
	c.events <- out
}

// terminalOnly notes the commands init says only Claude Code's terminal
// does, and lists the session's commands again without them.
func (c *conn) terminalOnly(names []string) {
	if len(names) == 0 {
		return
	}
	off := map[string]bool{}
	for _, n := range names {
		off[n] = true
	}
	c.mu.Lock()
	c.terminal = off
	list := c.commands
	c.mu.Unlock()
	if list != nil {
		c.listCommands(list)
	}
}
