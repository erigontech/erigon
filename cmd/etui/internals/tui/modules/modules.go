package modules

import (
	"fmt"
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

func Body() tview.Primitive {
	return tview.NewFlex().
		AddItem(tview.NewTextView().SetText("starting..."), 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(tview.NewTextView().SetText("starting1..."), 0, 1, false).
			AddItem(tview.NewTextView().SetText("starting2..."), 0, 1, false),
			0, 5, false)
}

func TextToBody(app *tview.Application, body *tview.Flex, infoCh <-chan *commands.StagesInfo) {
	for info := range infoCh {
		text := strconv.Itoa(rand.Int())
		app.QueueUpdateDraw(func() {
			infoView := body.GetItem(1).(*tview.Flex).GetItem(1).(*tview.Flex).GetItem(0).(*tview.TextView)
			infoView.Clear()
			fmt.Fprintf(infoView, "info %+v, text %s", info, text)
		})
		time.Sleep(time.Second * 5)
	}
}
