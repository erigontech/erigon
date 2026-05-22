package widgets

import (
	"fmt"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

// SystemHealthView holds the text views for the system health widget.
type SystemHealthView struct {
	CPU    *tview.TextView
	Memory *tview.TextView
	Disk   *tview.TextView
	DiskIO *tview.TextView
}

// NewSystemHealthWidget builds a horizontal flex containing system health metrics.
func NewSystemHealthWidget() (*tview.Flex, *SystemHealthView) {
	view := &SystemHealthView{
		CPU:    tview.NewTextView().SetDynamicColors(true).SetText("[::d]CPU: collecting...[-]"),
		Memory: tview.NewTextView().SetDynamicColors(true).SetText("[::d]RAM: collecting...[-]"),
		Disk:   tview.NewTextView().SetDynamicColors(true).SetText("[::d]Disk: collecting...[-]"),
		DiskIO: tview.NewTextView().SetDynamicColors(true).SetText("[::d]I/O: collecting...[-]"),
	}

	flex := tview.NewFlex().
		AddItem(view.CPU, 0, 1, false).
		AddItem(view.Memory, 0, 2, false).
		AddItem(view.Disk, 0, 2, false).
		AddItem(view.DiskIO, 0, 1, false)
	flex.Box.SetBorder(true).SetTitle(" System Health ")

	return flex, view
}

// UpdateSystemHealth formats and writes SystemStats + DiskIOPS into the view.
func (v *SystemHealthView) UpdateSystemHealth(stats datasource.SystemStats, iops datasource.DiskIOPS) {
	// CPU
	cpuColor := colorForPercent(stats.CPUPercent)
	v.CPU.SetText(fmt.Sprintf("[yellow]CPU:[-] %s%d%%[-] (%d cores)",
		cpuColor, stats.CPUPercent, stats.NumCPU))

	// Memory
	memPct := uint64(0)
	if stats.MemTotal > 0 {
		memPct = stats.MemUsed * 100 / stats.MemTotal
	}
	memColor := colorForPercent(memPct)
	v.Memory.SetText(fmt.Sprintf("[yellow]RAM:[-] %s%s / %s[-] | [yellow]Go heap:[-] %s",
		memColor,
		datasource.FormatBytes(stats.MemUsed),
		datasource.FormatBytes(stats.MemTotal),
		datasource.FormatBytes(stats.GoHeap)))

	// Disk
	diskPct := uint64(0)
	if stats.DiskTotal > 0 {
		diskPct = stats.DiskUsed * 100 / stats.DiskTotal
	}
	diskColor := colorForPercent(diskPct)
	v.Disk.SetText(fmt.Sprintf("[yellow]Disk:[-] %s%s / %s (%d%%)[-]",
		diskColor,
		datasource.FormatBytes(stats.DiskUsed),
		datasource.FormatBytes(stats.DiskTotal),
		diskPct))

	// Disk I/O
	v.DiskIO.SetText(fmt.Sprintf("[yellow]I/O:[-] [cyan]R:%d W:%d[-] iops",
		iops.Read, iops.Write))
}

// colorForPercent returns a tview color tag based on usage thresholds.
func colorForPercent(pct uint64) string {
	switch {
	case pct > 80:
		return "[red]"
	case pct > 60:
		return "[yellow]"
	default:
		return "[green]"
	}
}
