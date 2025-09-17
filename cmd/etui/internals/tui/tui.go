package tui

import (
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules"
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func MakeTUI(infoCh <-chan *commands.StagesInfo) error {
	app := tview.NewApplication()
	body, view := modules.Body()
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(modules.Header(), 1, 1, false).
		AddItem(body, 0, 5, false).
		AddItem(modules.Footer(), 5, 1, false)

	go modules.FillInfo(app, view, infoCh)

	if err := app.SetRoot(flex, true).EnableMouse(true).SetInputCapture(
		func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyCtrlC || event.Rune() == 'q' {
				app.Stop()
			}
			return event
		}).Run(); err != nil {
		return err
	}
	return nil
}
