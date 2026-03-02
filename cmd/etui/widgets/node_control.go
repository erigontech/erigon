package widgets

import (
	"fmt"
	"time"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

// NodeControlView displays the Erigon node status and Run/Stop indicator.
type NodeControlView struct {
	*tview.TextView
}

// NewNodeControlView creates the node control widget.
func NewNodeControlView() *NodeControlView {
	tv := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter)
	tv.SetBorder(true).SetTitle(" Node ")
	tv.SetText("[::d]Detecting...[-]")
	return &NodeControlView{TextView: tv}
}

// UpdateNodeStatus renders the current node status.
func (v *NodeControlView) UpdateNodeStatus(status datasource.NodeStatus) {
	var indicator, stateLabel, detail string

	switch status.State {
	case datasource.NodeRunning:
		indicator = "[green]●[-]"
		stateLabel = "[green]Running[-]"
		detail = fmt.Sprintf(" PID %d  %s  [yellow]R[-]=stop", status.PID, formatUptime(status.Uptime))
	case datasource.NodeExternal:
		indicator = "[blue]●[-]"
		stateLabel = "[blue]Running (external)[-]"
		if status.PID > 0 {
			detail = fmt.Sprintf("  PID %d", status.PID)
		}
	case datasource.NodeStarting:
		indicator = "[yellow]●[-]"
		stateLabel = "[yellow]Starting...[-]"
		detail = fmt.Sprintf(" PID %d", status.PID)
	case datasource.NodeStopping:
		indicator = "[yellow]●[-]"
		stateLabel = "[yellow]Stopping...[-]"
		detail = ""
	default: // Stopped
		indicator = "[gray]●[-]"
		stateLabel = "[gray]Stopped[-]"
		if status.ExitErr != "" {
			detail = fmt.Sprintf("  [red]%s[-]  [yellow]R[-]=start", status.ExitErr)
		} else {
			detail = "  [yellow]R[-]=start"
		}
	}

	v.SetText(fmt.Sprintf("%s %s%s", indicator, stateLabel, detail))
}

func formatUptime(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

// ConfirmDialog shows a modal confirmation dialog.
// onConfirm is called if the user selects "Yes".
// onCancel is called if the user selects "No" or presses Escape.
func ConfirmDialog(title, message string, onConfirm, onCancel func()) *tview.Modal {
	modal := tview.NewModal().
		SetText(message).
		AddButtons([]string{"Yes", "No"}).
		SetDoneFunc(func(_ int, buttonLabel string) {
			if buttonLabel == "Yes" {
				onConfirm()
			} else {
				onCancel()
			}
		})
	modal.SetTitle(title).SetBorder(true)
	return modal
}
