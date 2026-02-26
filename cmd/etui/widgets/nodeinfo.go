package widgets

import (
	"github.com/rivo/tview"
)

// NodeInfoView holds the text views for the node-info page.
type NodeInfoView struct {
	Overview   *tview.TextView
	Stages     *tview.TextView
	DomainII   *tview.TextView
	Clock      *tview.TextView
	Downloader *tview.TextView
}

// NewNodeInfoPage builds the node-info page layout and returns it with its backing view.
func NewNodeInfoPage() (*tview.Flex, *NodeInfoView) {
	view := &NodeInfoView{
		Overview:   tview.NewTextView().SetText("waiting for fetch data from erigon...").SetDynamicColors(true),
		Stages:     tview.NewTextView().SetDynamicColors(true),
		DomainII:   tview.NewTextView().SetDynamicColors(true),
		Clock:      tview.NewTextView().SetTextAlign(tview.AlignRight).SetDynamicColors(true),
		Downloader: tview.NewTextView().SetDynamicColors(true).SetText("downloader: waiting..."),
	}

	view.Downloader.Box.SetBorder(true)
	topPanel := tview.NewFlex().
		AddItem(view.Overview, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(view.Clock, 1, 1, false).
			AddItem(view.Downloader, 0, 5, false), 0, 1, false)
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topPanel, 9, 1, false).
		AddItem(tview.NewFlex().
			AddItem(view.Stages, 0, 1, false).
			AddItem(view.DomainII, 0, 1, false),
			0, 1, false)
	flex.Box.SetBorder(true)
	return flex, view
}
