package claude

import (
	"strings"

	"github.com/0xdeafcafe/rush/internal/agent"
)

// SessionAPIFeatures advertises only capabilities the headless adapter can
// confirm and control through the public host API.
func (Adapter) SessionAPIFeatures() []string {
	return []string{"explicit_settings", "auto_permissions", "structured_questions", "image_input"}
}

func (a Adapter) SessionAPIProfile() agent.Profile { return a.Home() }

func (Adapter) SessionAPIModel(model string) string {
	for _, alias := range []string{"opus", "sonnet", "haiku", "fable"} {
		if model == alias || strings.Contains(model, "-"+alias+"-") {
			return alias
		}
	}
	return model
}

func (Adapter) SessionAPIMode(mode string) string {
	if mode == "default" {
		return "ask"
	}
	return mode
}

func (Adapter) SessionAPIEngineMode(mode string) string {
	if mode == "ask" {
		return "default"
	}
	return mode
}

var _ agent.SessionAPIProvider = Adapter{}
