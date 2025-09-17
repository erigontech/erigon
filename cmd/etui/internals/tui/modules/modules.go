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

func Body(info *commands.StagesInfo) tview.Primitive {
	return tview.NewTextView().SetText(fmt.Sprintf("%+v", info))
}

func TextToBody(body *tview.TextView) {
	for {
		text := strconv.Itoa(rand.Int())
		body.SetText(text)
		time.Sleep(time.Millisecond * 200)
	}
}
