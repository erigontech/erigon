package tui

import (
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules"
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func MakeTUI(info *commands.StagesInfo) {
	app := tview.NewApplication()
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(modules.Header(), 1, 1, false).
		AddItem(modules.Body(info), 0, 5, false).
		AddItem(modules.Footer(), 5, 1, false)

	if err := app.SetRoot(flex, true).EnableMouse(true).SetInputCapture(
		func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyCtrlC || event.Rune() == 'q' {
				app.Stop()
			}
			return event
		}).Run(); err != nil {
		panic(err)
	}
}
