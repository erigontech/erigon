package startpage

import (
	"fmt"
	"github.com/rivo/tview"
)

const E3Logo = ` ________            __                                       ______  
/        |          /  |                                     /      \ 
$$$$$$$$/   ______  $$/   ______    ______   _______        /$$$$$$  |
$$ |__     /      \ /  | /      \  /      \ /       \       $$ ___$$ |
$$    |   /$$$$$$  |$$ |/$$$$$$  |/$$$$$$  |$$$$$$$  |        /   $$< 
$$$$$/    $$ |  $$/ $$ |$$ |  $$ |$$ |  $$ |$$ |  $$ |       _$$$$$  |
$$ |_____ $$ |      $$ |$$ \__$$ |$$ \__$$ |$$ |  $$ |      /  \__$$ |
$$       |$$ |      $$ |$$    $$ |$$    $$/ $$ |  $$ |      $$    $$/ 
$$$$$$$$/ $$/       $$/  $$$$$$$ | $$$$$$/  $$/   $$/        $$$$$$/  
                        /  \__$$ |                                    
                        $$    $$/                                     
                         $$$$$$/                                      `

func Body(clock *tview.TextView, datadir string) (*tview.Flex, *BodyView) {
	netInf := tview.NewTextView().SetDynamicColors(true).SetText("network info...")
	view := &BodyView{
		Logo:        tview.NewTextView().SetText(E3Logo).SetDynamicColors(true),
		Network:     NetworkDropdown(netInf),
		NetworkInfo: netInf,
		Execution:   tview.NewTextView().SetDynamicColors(true).SetText("exec/stop"),
		Status:      tview.NewTextView().SetDynamicColors(true).SetText("status..."),
		Clock:       clock,
		Datadir: tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignRight).
			SetText(fmt.Sprintf("datadir: %s", datadir)),
	}

	topPanel := tview.NewFlex().
		AddItem(view.Logo, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(view.Clock, 1, 1, false).
			AddItem(view.Datadir, 0, 5, false), 0, 1, false)
	//topPanel.GetItem(1).(*tview.Flex).GetItem(1).(*tview.TextView).Box.SetBorder(true)
	networkWidget := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(view.Network, 0, 1, false).
		AddItem(view.NetworkInfo, 0, 1, false)
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topPanel,
			15, 1, false).
		AddItem(tview.NewFlex().
			AddItem(networkWidget, 0, 1, false).
			AddItem(view.Status, 0, 1, false).
			AddItem(view.Execution, 0, 1, false),
			0, 1, false)
	flex.Box.SetBorder(true)
	return flex, view
}

type BodyView struct {
	Logo        *tview.TextView
	Network     *tview.DropDown
	NetworkInfo *tview.TextView
	Datadir     *tview.TextView
	Execution   *tview.TextView
	Clock       *tview.TextView
	Status      *tview.TextView
}

var Networks = []string{"mainnet", "hoodi", "sepolia"}
var Network = "mainnet"

func NetworkDropdown(netInf *tview.TextView) *tview.DropDown {
	dd := tview.NewDropDown().SetLabel("choose network: ").
		SetOptions(Networks, func(text string, index int) {
			Network = Networks[index]
			netInf.SetText(Network)
		}).SetCurrentOption(0)
	return dd
}
