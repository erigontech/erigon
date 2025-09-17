package modules

import (
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/rivo/tview"
	"math/rand"
	"strconv"
	"time"
)

func Header() tview.Primitive {
	return tview.NewTextView().SetText("Erigon TUI").SetTextAlign(tview.AlignCenter)
}

func Footer() tview.Primitive {
	return tview.NewBox().SetBorder(true).SetTitle("Bottom (5 rows)")
}

func Body() (tview.Primitive, *BodyView) {
	view := &BodyView{
		Overview: tview.NewTextView().SetText("starting..."),
		Stages:   tview.NewTextView().SetText("starting1..."),
		DomainII: tview.NewTextView().SetText("starting2..."),
	}
	return tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(view.Overview, 0, 1, false).
		AddItem(tview.NewFlex().
			AddItem(view.Stages, 0, 1, false).
			AddItem(view.DomainII, 0, 1, false),
			0, 5, false), view
}

type BodyView struct {
	Overview *tview.TextView
	Stages   *tview.TextView
	DomainII *tview.TextView
}

func FillInfo(app *tview.Application, body *BodyView, infoCh <-chan *commands.StagesInfo) {
	for info := range infoCh {
		text := strconv.Itoa(rand.Int())
		app.QueueUpdateDraw(func() {
			body.Overview.Clear()
			body.Overview.SetText(info.Overview() + text)
			body.Stages.Clear()
			body.Stages.SetText(info.Stages())
			body.DomainII.Clear()
			body.DomainII.SetText(info.DomainII())
		})
		time.Sleep(time.Second * 5)
	}
}
