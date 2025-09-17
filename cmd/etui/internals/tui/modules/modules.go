package modules

import (
	"fmt"
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/rivo/tview"
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
