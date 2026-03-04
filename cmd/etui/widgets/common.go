package widgets

import (
	"fmt"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
)

// Header returns the top-of-screen header bar.
// mode is the current app mode (e.g. "Analytics", "Standalone"), or "" for no indicator.
func Header(mode string) tview.Primitive {
	tv := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter)
	if mode != "" {
		tv.SetText(fmt.Sprintf("Erigon TUI v%s    [yellow][%s Mode][-]", config.Version, mode))
	} else {
		tv.SetText(fmt.Sprintf("Erigon TUI v%s", config.Version))
	}
	return tv
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
		SetText("[green]◄ ► [-]navigate  [green]L [-]logs  " + rHint + "[green]C [-]config  [yellow]Press [red]q [yellow]or [red]Ctrl+C [yellow]to quit")
}
