package widgets

import (
	"github.com/rivo/tview"
)

// NodeInfoView holds the text views for the node-info page.
type NodeInfoView struct {
	SyncStatus   *SyncStatusView
	Stages       *tview.TextView
	DomainII     *tview.TextView
	Clock        *tview.TextView
	Downloader   *tview.TextView
	SystemHealth *SystemHealthView
}

// NewNodeInfoPage builds the node-info page layout and returns it with its backing view.
func NewNodeInfoPage() (*tview.Flex, *NodeInfoView) {
	view := &NodeInfoView{
		SyncStatus: NewSyncStatusView(),
		Stages:     tview.NewTextView().SetDynamicColors(true),
		DomainII:   tview.NewTextView().SetDynamicColors(true),
		Clock:      tview.NewTextView().SetTextAlign(tview.AlignRight).SetDynamicColors(true),
		Downloader: tview.NewTextView().SetDynamicColors(true).SetText("downloader: waiting..."),
	}

	sysHealthFlex, sysHealthView := NewSystemHealthWidget()
	view.SystemHealth = sysHealthView

	view.Downloader.Box.SetBorder(true)
	topPanel := tview.NewFlex().
		AddItem(view.SyncStatus.TextView, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(view.Clock, 1, 1, false).
			AddItem(view.Downloader, 0, 5, false), 0, 1, false)

	middlePanel := tview.NewFlex().
		AddItem(view.Stages, 0, 1, false).
		AddItem(view.DomainII, 0, 1, false)

	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topPanel, 9, 1, false).
		AddItem(middlePanel, 0, 1, false).
		AddItem(sysHealthFlex, 3, 0, false)
	flex.Box.SetBorder(true)
	return flex, view
}
