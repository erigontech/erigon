package widgets

import (
	"fmt"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

// SyncStatusView displays a human-readable sync dashboard:
// Stage, Head, Finalized, Tip lag, Import rate, with color-coded status.
type SyncStatusView struct {
	*tview.TextView
}

// NewSyncStatusView creates a new sync-status widget.
func NewSyncStatusView() *SyncStatusView {
	tv := tview.NewTextView().
		SetDynamicColors(true).
		SetText("[::d]waiting for sync data...[-]")
	return &SyncStatusView{TextView: tv}
}

// UpdateSyncStatus refreshes the widget with new metrics.
func (v *SyncStatusView) UpdateSyncStatus(m datasource.SyncMetrics, stageName string) {
	statusColor := statusToColor(m.Status)
	statusLabel := m.Status.String()

	lagStr := formatLag(m.TipLagSeconds)
	rateStr := formatRate(m.ImportRate)

	text := fmt.Sprintf(
		"[yellow]Status:[-]     [%s::b]● %s[-:-:-]\n"+
			"[yellow]Stage:[-]      [cyan]%s[-]\n"+
			"[yellow]Head:[-]       [cyan]%d[-]\n"+
			"[yellow]Pipeline:[-]   [cyan]%d[-]\n"+
			"[yellow]Tip lag:[-]    %s\n"+
			"[yellow]Import:[-]     %s",
		statusColor, statusLabel,
		stageName,
		m.HeadBlock,
		m.PipelineHead,
		lagStr,
		rateStr,
	)
	v.Clear()
	v.SetText(text)
}

// statusToColor returns a tview color tag for the sync status.
func statusToColor(s datasource.SyncStatus) string {
	switch s {
	case datasource.StatusInSync:
		return "green"
	case datasource.StatusSyncing:
		return "yellow"
	case datasource.StatusStalled:
		return "red"
	default:
		return "gray"
	}
}

// formatLag formats tip lag with color coding.
func formatLag(seconds int64) string {
	if seconds < 0 {
		return "[::d]—[-]"
	}
	if seconds < 30 {
		return fmt.Sprintf("[green]%ds[-]", seconds)
	}
	if seconds < 60 {
		return fmt.Sprintf("[yellow]%ds[-]", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("[red]%dm%ds[-]", seconds/60, seconds%60)
	}
	return fmt.Sprintf("[red]%dh%dm[-]", seconds/3600, (seconds%3600)/60)
}

// formatRate formats import rate (blocks/minute).
func formatRate(rate float64) string {
	if rate < 0.1 {
		return "[::d]—[-]"
	}
	if rate >= 1000 {
		return fmt.Sprintf("[green]%.0f blk/min[-]", rate)
	}
	return fmt.Sprintf("[green]%.1f blk/min[-]", rate)
}
