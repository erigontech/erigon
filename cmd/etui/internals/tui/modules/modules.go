package modules

import (
	"github.com/rivo/tview"
)

func Header() tview.Primitive {
	return tview.NewTextView().SetText("Erigon TUI v0.0.2").SetTextAlign(tview.AlignCenter)
}

func Footer() *tview.TextView {
	return tview.NewTextView().SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter).
		SetText("[green]navigation left right arrows [yellow]Press [red]q [yellow]or [red]Ctrl+C [yellow]to quit")
}
