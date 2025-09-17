package modules

import (
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/rivo/tview"
	"time"
)

func Header() tview.Primitive {
	return tview.NewTextView().SetText("Erigon TUI").SetTextAlign(tview.AlignCenter)
}

func Footer() tview.Primitive {
	return tview.NewTextView().SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter).
		SetText("[yellow]Press [red]q [yellow]or [red]Ctrl+C [yellow]to quit")
}

func Body() (*tview.Flex, *BodyView) {
	view := &BodyView{
		Overview: tview.NewTextView().SetText("starting..."),
		Stages:   tview.NewTextView().SetText("starting1..."),
		DomainII: tview.NewTextView().SetText("starting2..."),
		Clock:    tview.NewTextView().SetText("starting3...").SetTextAlign(tview.AlignRight),
	}
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(tview.NewFlex().
			AddItem(view.Overview, 0, 1, false).
			AddItem(view.Clock, 0, 1, false), 0, 1, false).
		AddItem(tview.NewFlex().
			AddItem(view.Stages, 0, 1, false).
			AddItem(view.DomainII, 0, 1, false),
			0, 5, false)
	flex.Box.SetBorder(true)
	view.Clock.Box.SetBorder(true).SetTitle("Clock")
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
			body.Overview.SetText(info.Overview())
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
		// обновление строго через QueueUpdateDraw
		now := t.Format("15:04:05")
		app.QueueUpdateDraw(func() {
			clock.SetText(now)
		})
	}
}
