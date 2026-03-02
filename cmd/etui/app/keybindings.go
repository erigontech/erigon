package app

import (
	"context"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
	"github.com/erigontech/erigon/cmd/etui/widgets"
)

const pageConfirm = "confirm"

// handleNodeToggle is called when the user presses R on a dashboard page.
// It either starts or stops the Erigon node, showing a confirmation dialog
// for destructive actions (stop).
func (a *App) handleNodeToggle(
	ctx context.Context,
	pages *tview.Pages,
	nodeMgr *datasource.NodeManager,
	nodeView *widgets.NodeControlView,
	returnPage string,
) {
	status := nodeMgr.Status()
	a.log.Info("R pressed: node state=%s pid=%d", status.State, status.PID)

	switch status.State {
	case datasource.NodeExternal:
		// Cannot control an external node — flash a message.
		// NOTE: We're already on the tview event loop (InputCapture), so we
		// call SetText directly. QueueUpdateDraw would deadlock because it
		// blocks until the event loop processes the update, but WE are the
		// event loop.
		nodeView.SetText("[blue]●[-] [blue]Running (external)[-] — cannot control from TUI")
		return

	case datasource.NodeStarting, datasource.NodeStopping:
		// Already transitioning — ignore.
		return

	case datasource.NodeRunning:
		// Show confirmation dialog before stopping.
		modal := widgets.ConfirmDialog(
			" Stop Node ",
			"Stop the Erigon node? This will send SIGTERM to the running process.",
			func() {
				// Confirmed — stop in background so we don't block the event loop.
				pages.RemovePage(pageConfirm)
				pages.SwitchToPage(returnPage)
				go func() {
					_ = nodeMgr.Stop()
				}()
			},
			func() {
				// Cancelled.
				pages.RemovePage(pageConfirm)
				pages.SwitchToPage(returnPage)
			},
		)
		pages.AddPage(pageConfirm, modal, true, true)
		a.tview.SetFocus(modal)

	default: // NodeStopped
		// Show immediate feedback — we're on the event loop, so call directly.
		nodeView.SetText("[yellow]●[-] Starting...")
		// Start in background — Start() performs blocking syscalls (stat, open,
		// exec) and must not run on the tview event loop.
		go func() {
			if err := nodeMgr.Start(ctx); err != nil {
				a.log.Error("node start failed: %v", err)
				// From a goroutine, QueueUpdateDraw is correct and required.
				a.tview.QueueUpdateDraw(func() {
					nodeView.SetText("[red]●[-] [red]Start failed:[-] " + err.Error())
				})
			}
		}()
	}
}
