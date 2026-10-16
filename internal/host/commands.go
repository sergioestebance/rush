package host

import (
	"os"
	"path/filepath"

	"github.com/0xdeafcafe/photon/jsonx"
	"github.com/0xdeafcafe/rush/internal/agent/event"
)

// commandsPath is where a session keeps the last slash commands its agent
// listed, so a client of a session whose agent isn't running still gets
// them.
func commandsPath(id string) string { return filepath.Join(dir(id), "commands.json") }

// WriteCommands saves a session's slash commands.
func WriteCommands(id string, list []event.Command) error {
	b, err := jsonx.Marshal(list)
	if err != nil {
		return err
	}
	tmp := commandsPath(id) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, commandsPath(id))
}

// ReadCommands is a session's saved slash commands, nil when it has none.
func ReadCommands(id string) []event.Command {
	b, err := os.ReadFile(commandsPath(id))
	if err != nil {
		return nil
	}
	var list []event.Command
	if jsonx.Unmarshal(b, &list) != nil {
		return nil
	}
	return list
}

// loadCommands takes the saved list as the one clients get, until the
// agent lists its own.
func (s *server) loadCommands() {
	if list := ReadCommands(s.cfg.ID); list != nil {
		s.commands = commandsLine(list)
	}
}

// commandsLine is a list of commands as the line clients get.
func commandsLine(list []event.Command) []byte {
	wire := make([]wireCommand, len(list))
	for i, c := range list {
		wire[i] = wireCommand(c)
	}
	b, _ := jsonx.Marshal(map[string]any{"type": typeCommands, "commands": wire})
	return b
}
