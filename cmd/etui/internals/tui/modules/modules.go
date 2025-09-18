package modules

import (
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/rivo/tview"
	"time"
)

func Header() tview.Primitive {
	return tview.NewTextView().SetText("Erigon TUI v0.0.1 print_stages").SetTextAlign(tview.AlignCenter)
}

func Footer() *tview.TextView {
	return tview.NewTextView().SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter).
		SetText("[yellow]Press [red]q [yellow]or [red]Ctrl+C [yellow]to quit")
}

func Body() (*tview.Flex, *BodyView) {
	view := &BodyView{
		Overview: tview.NewTextView().SetText("waiting for fetch data from erigon...").SetDynamicColors(true),
		Stages:   tview.NewTextView().SetDynamicColors(true),
		DomainII: tview.NewTextView().SetDynamicColors(true),
		Clock:    tview.NewTextView().SetTextAlign(tview.AlignRight).SetDynamicColors(true),
	}
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(tview.NewFlex().
			AddItem(view.Overview, 0, 1, false).
			AddItem(view.Clock, 0, 1, false), 8, 1, false).
		AddItem(tview.NewFlex().
			AddItem(view.Stages, 0, 1, false).
			AddItem(view.DomainII, 0, 1, false),
			0, 5, false)
	flex.Box.SetBorder(true)
	return flex, view
}

type BodyView struct {
	Overview *tview.TextView
	Stages   *tview.TextView
	DomainII *tview.TextView
	Clock    *tview.TextView
}

func FillInfo(app *tview.Application, body *BodyView, infoCh <-chan *commands.StagesInfo) {
	for info := range infoCh {
		app.QueueUpdateDraw(func() {
			body.Overview.Clear()
			body.Overview.SetText(info.OverviewTUI())
			body.Stages.Clear()
			body.Stages.SetText(info.Stages())
			body.DomainII.Clear()
			body.DomainII.SetText(info.DomainII())
		})
	}
}

func Clock(app *tview.Application, clock *tview.TextView) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for t := range ticker.C {
		now := t.Format("15:04:05")
		app.QueueUpdateDraw(func() {
			clock.SetText(now)
		})
	}
}

func HandleErrors(app *tview.Application, errCh <-chan error, view *tview.TextView) {
	if err, ok := <-errCh; ok && err != nil {
		app.QueueUpdateDraw(func() {
			// пример: подсветить ошибку в футере/статус-баре
			view.SetDynamicColors(true)
			view.SetText(
				"[red::b]ERROR:[-] " + err.Error() + "   [::d](press q or Ctrl+C to quit)[-]",
			)
		})
		time.AfterFunc(5*time.Second, app.Stop)
	}
}
