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
	return tview.NewTextView().SetText("starting...")
}

func TextToBody(app *tview.Application, body *tview.Flex, infoCh <-chan *commands.StagesInfo) {
	for info := range infoCh {
		text := strconv.Itoa(rand.Int())
		app.QueueUpdateDraw(func() {
			infoView := body.GetItem(1).(*tview.TextView)
			infoView.Clear()
			fmt.Fprintf(infoView, "info %+v, text %s", info, text)
		})
		time.Sleep(time.Second * 5)
	}
}
