package widgets

import (
	"fmt"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
)

// Header returns the top-of-screen header bar.
func Header() tview.Primitive {
	return tview.NewTextView().
		SetText(fmt.Sprintf("Erigon TUI v%s", config.Version)).
		SetTextAlign(tview.AlignCenter)
}

// Footer returns the bottom-of-screen help bar.
func Footer() *tview.TextView {
	return tview.NewTextView().SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter).
		SetText("[green]navigation left right arrows [yellow]Press [red]q [yellow]or [red]Ctrl+C [yellow]to quit")
}
