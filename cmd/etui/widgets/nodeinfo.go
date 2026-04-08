package widgets

import (
	"path/filepath"
	"runtime"

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
	NodeControl  *NodeControlView
	middlePanel  *tview.Flex
	domainShown  bool
}

// NewNodeInfoPage builds the node-info page layout and returns it with its backing view.
// datadir is used to derive the default log path (datadir/logs/erigon.log).
func NewNodeInfoPage(datadir string) (*tview.Flex, *NodeInfoView) {
	logPath := filepath.Join(datadir, "logs", "erigon.log")
	stagesPlaceholder := "[::d]waiting for chaindata initialization...[-]"
	domainPlaceholder := "[::d]stage database is not available yet[-]"
	if runtime.GOOS == "windows" {
		stagesPlaceholder = "[::d]stage progress is unavailable in Windows builds[-]"
		domainPlaceholder = "[::d]domain/index progress is unavailable in Windows builds[-]"
	}

	view := &NodeInfoView{
		SyncStatus:  NewSyncStatusView(),
		Stages:      tview.NewTextView().SetDynamicColors(true).SetText(stagesPlaceholder),
		DomainII:    tview.NewTextView().SetDynamicColors(true).SetText(domainPlaceholder),
		Clock:       tview.NewTextView().SetTextAlign(tview.AlignRight).SetDynamicColors(true),
		Downloader:  NewDownloaderView(),
		Alerts:      NewAlertsView(),
		LogTail:     NewLogTailView(logPath, 20),
		NodeControl: NewNodeControlView(),
	}

	sysHealthFlex, sysHealthView := NewSystemHealthWidget()
	view.SystemHealth = sysHealthView

	// Top: SyncStatus | Clock + NodeControl + Downloader
	topPanel := tview.NewFlex().
		AddItem(view.SyncStatus.TextView, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(view.Clock, 1, 1, false).
			AddItem(view.NodeControl.TextView, 3, 0, false).
			AddItem(view.Downloader.TextView, 0, 5, false), 0, 1, false)

	// Middle: Stages | Domain/II
	middlePanel := tview.NewFlex().
		AddItem(view.Stages, 0, 1, false)
	view.middlePanel = middlePanel

	// Bottom: SystemHealth | Alerts (side by side)
	bottomPanel := tview.NewFlex().
		AddItem(sysHealthFlex, 0, 1, false).
		AddItem(view.Alerts.TextView, 0, 1, false)

	// Layout: top(12) + middle(flex) + bottom(5) + log_tail(4)
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topPanel, 12, 1, false).
		AddItem(middlePanel, 0, 1, false).
		AddItem(bottomPanel, 5, 0, false).
		AddItem(view.LogTail.TextView, 4, 0, false)
	flex.Box.SetBorder(true)
	return flex, view
}

// SetDomainIIVisible collapses or restores the secondary Domain/II panel.
func (v *NodeInfoView) SetDomainIIVisible(visible bool) {
	if v.middlePanel == nil || v.domainShown == visible {
		return
	}
	if visible {
		v.middlePanel.AddItem(v.DomainII, 0, 1, false)
	} else {
		v.middlePanel.RemoveItem(v.DomainII)
	}
	v.domainShown = visible
}
