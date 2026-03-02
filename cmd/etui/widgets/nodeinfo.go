package widgets

import (
	"path/filepath"

	"github.com/rivo/tview"
)

// NodeInfoView holds the text views for the node-info page.
type NodeInfoView struct {
	SyncStatus   *SyncStatusView
	Stages       *tview.TextView
	DomainII     *tview.TextView
	Clock        *tview.TextView
	Downloader   *DownloaderView
	SystemHealth *SystemHealthView
	Alerts       *AlertsView
	LogTail      *LogTailView
}

// NewNodeInfoPage builds the node-info page layout and returns it with its backing view.
// datadir is used to derive the default log path (datadir/logs/erigon.log).
func NewNodeInfoPage(datadir string) (*tview.Flex, *NodeInfoView) {
	logPath := filepath.Join(datadir, "logs", "erigon.log")

	view := &NodeInfoView{
		SyncStatus: NewSyncStatusView(),
		Stages:     tview.NewTextView().SetDynamicColors(true),
		DomainII:   tview.NewTextView().SetDynamicColors(true),
		Clock:      tview.NewTextView().SetTextAlign(tview.AlignRight).SetDynamicColors(true),
		Downloader: NewDownloaderView(),
		Alerts:     NewAlertsView(),
		LogTail:    NewLogTailView(logPath, 20),
	}

	sysHealthFlex, sysHealthView := NewSystemHealthWidget()
	view.SystemHealth = sysHealthView

	// Top: SyncStatus | Clock + Downloader
	topPanel := tview.NewFlex().
		AddItem(view.SyncStatus.TextView, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(view.Clock, 1, 1, false).
			AddItem(view.Downloader.TextView, 0, 5, false), 0, 1, false)

	// Middle: Stages | Domain/II
	middlePanel := tview.NewFlex().
		AddItem(view.Stages, 0, 1, false).
		AddItem(view.DomainII, 0, 1, false)

	// Bottom: SystemHealth | Alerts (side by side)
	bottomPanel := tview.NewFlex().
		AddItem(sysHealthFlex, 0, 1, false).
		AddItem(view.Alerts.TextView, 0, 1, false)

	// Layout: top(9) + middle(flex) + bottom(5) + log_tail(4)
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topPanel, 9, 1, false).
		AddItem(middlePanel, 0, 1, false).
		AddItem(bottomPanel, 5, 0, false).
		AddItem(view.LogTail.TextView, 4, 0, false)
	flex.Box.SetBorder(true)
	return flex, view
}
