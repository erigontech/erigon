package widgets

import (
	"fmt"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
)

type HeaderView struct {
	*tview.TextView
	mode string
	role string
}

// Header returns the top-of-screen header bar.
// mode is the current app mode (e.g. "Analytics", "Standalone"), or "" for no indicator.
func Header(mode string) *HeaderView {
	tv := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter)
	header := &HeaderView{
		TextView: tv,
		mode:     mode,
		role:     "Full+RPC",
	}
	header.render()
	return header
}

func (h *HeaderView) SetRole(role string) {
	if role == "" {
		role = "Full+RPC"
	}
	h.role = role
	h.render()
}

func (h *HeaderView) render() {
	mode := ""
	if h.mode != "" {
		mode = fmt.Sprintf("    [yellow][%s Mode][-]", h.mode)
	}
	roleColor := "cyan"
	if h.role == "Validator" {
		roleColor = "green"
	}
	h.SetText(fmt.Sprintf("Erigon TUI v%s%s    [%s][%s][-]", config.Version, mode, roleColor, h.role))
}

// Footer returns the bottom-of-screen help bar.
// standalone controls whether the R=node hint is shown (only in Standalone mode).
func Footer(standalone bool) *tview.TextView {
	rHint := ""
	if standalone {
		rHint = "[green]R [-]node  "
	}
	return tview.NewTextView().SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter).
		SetText("[green]◄ ► [-]navigate  [green]F4 [-]validator  [green]L [-]logs  " + rHint + "[green]C [-]config  [yellow]Press [red]q [yellow]or [red]Ctrl+C [yellow]to quit")
}
