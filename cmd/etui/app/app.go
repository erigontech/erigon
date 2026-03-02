package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
	"github.com/erigontech/erigon/cmd/etui/datasource"
	"github.com/erigontech/erigon/cmd/etui/widgets"
	"github.com/erigontech/erigon/cmd/integration/commands"
)

// App is the top-level TUI application.
type App struct {
	tview       *tview.Application
	dp          *datasource.DownloaderPinger
	dlTracker   *datasource.DownloaderTracker
	sysColl     *datasource.SystemCollector
	iopsTrack   *datasource.DiskIOPSTracker
	syncTracker *datasource.SyncTracker
	alertMgr    *datasource.AlertManager
	logTailer   *datasource.LogTailer
	nodeMgr     *datasource.NodeManager
	log         *datasource.TUILog
	datadir     string
}

// New creates an App that reads from the given datadir.
func New(datadir string) *App {
	logPath := filepath.Join(datadir, "logs", "erigon.log")
	tuiLog := datasource.NewTUILog(datadir)
	tuiLog.Info("etui starting, datadir=%s", datadir)
	return &App{
		datadir:     datadir,
		tview:       tview.NewApplication(),
		dp:          datasource.NewDownloaderPinger(config.DefaultDownloaderURL),
		dlTracker:   datasource.NewDownloaderTracker(),
		sysColl:     datasource.NewSystemCollector(datadir),
		iopsTrack:   datasource.NewDiskIOPSTracker(),
		syncTracker: datasource.NewSyncTracker(),
		alertMgr:    datasource.NewAlertManager(),
		logTailer:   datasource.NewLogTailer(logPath),
		nodeMgr:     datasource.NewNodeManager(datadir, ""),
		log:         tuiLog,
	}
}

// Page name constants.
const (
	pageStart    = "start"
	pageNodeInfo = "nodeInfo"
	pageLogs     = "logs"
)

// Run starts the TUI event loop. It blocks until the user quits or the parent
// context is cancelled (e.g. by an OS signal).
func (a *App) Run(parent context.Context, infoCh <-chan *commands.StagesInfo, errCh chan error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer a.log.Close()

	// Build pages
	pages := tview.NewPages()

	nodeInfoBody, nodeView := widgets.NewNodeInfoPage(a.datadir)
	footer := widgets.Footer()

	startPageBody, _ := widgets.NewStartPage(nodeView.Clock, a.datadir)

	startPage := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(widgets.Header(), 1, 1, false).
		AddItem(startPageBody, 0, 5, false).
		AddItem(footer, 2, 1, false)
	nodeInfoPage := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(widgets.Header(), 1, 1, false).
		AddItem(nodeInfoBody, 0, 5, false).
		AddItem(footer, 2, 1, false)

	// All navigable pages, including the full-screen log viewer.
	// ◄ ► cycles through them; F2/L jumps directly to logs.
	dashPages := []string{pageStart, pageNodeInfo, pageLogs}
	currentPage := 0
	const logsPageIdx = 2 // index of pageLogs in dashPages

	// Declare logViewer before closures that reference it.
	var logViewer *widgets.LogViewerPage

	// navigateToPage switches to the page at idx and sets focus correctly.
	// Must only be called from the tview event loop (InputCapture).
	navigateToPage := func(idx int) {
		currentPage = idx
		pages.SwitchToPage(dashPages[idx])
		if dashPages[idx] == pageLogs {
			a.tview.SetFocus(logViewer.Content())
		} else {
			a.tview.SetFocus(pages)
		}
	}

	switchToDashboard := func() {
		navigateToPage(1) // nodeInfo
	}
	logViewer = widgets.NewLogViewerPage(switchToDashboard)
	logsPage := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(widgets.Header(), 1, 1, false).
		AddItem(logViewer.Root, 0, 1, true)

	// Seed the log tailer so the viewer has content immediately.
	a.logTailer.SeedFromEnd()

	// Detect if an external node is already running.
	a.nodeMgr.DetectExternal()
	initStatus := a.nodeMgr.Status()
	a.log.Info("initial node state: %s pid=%d", initStatus.State, initStatus.PID)

	// Start background goroutines
	go a.safeGo("fillStagesInfo", errCh, func() { a.fillStagesInfo(ctx, nodeView, infoCh) })
	go a.safeGo("runClock", errCh, func() { a.runClock(ctx, nodeView.Clock) })
	go a.handleErrors(ctx, errCh, footer) // not wrapped — it drains errCh itself
	go a.safeGo("pollDownloader", errCh, func() { a.pollDownloader(ctx, nodeView.Downloader, errCh) })
	go a.safeGo("pollSystemHealth", errCh, func() { a.pollSystemHealth(ctx, nodeView.SystemHealth) })
	go a.safeGo("pollAlerts", errCh, func() { a.pollAlerts(ctx, nodeView.Alerts) })
	go a.safeGo("pollLogTail", errCh, func() { a.pollLogTail(ctx, nodeView.LogTail) })
	go a.safeGo("pollLogViewer", errCh, func() { a.pollLogViewer(ctx, logViewer) })
	go a.safeGo("pollNodeStatus", errCh, func() { a.pollNodeStatus(ctx, nodeView.NodeControl) })

	pages.AddPage(pageStart, startPage, true, true)
	pages.AddPage(pageNodeInfo, nodeInfoPage, true, false)
	pages.AddPage(pageLogs, logsPage, true, false)

	if err := a.tview.SetRoot(pages, true).EnableMouse(true).SetInputCapture(
		func(event *tcell.EventKey) *tcell.EventKey {
			currentFront, _ := pages.GetFrontPage()

			// --- Log viewer search mode: capture all input ---
			if currentFront == pageLogs && logViewer.IsSearching() {
				if event.Key() == tcell.KeyEscape {
					logViewer.DismissSearch()
					a.tview.SetFocus(logViewer.Content())
					return nil
				}
				if event.Key() == tcell.KeyEnter {
					logViewer.DismissSearch()
					a.tview.SetFocus(logViewer.Content())
					return nil
				}
				return event // let InputField handle typing
			}

			// --- Global keys (all pages) ---
			switch {
			case event.Key() == tcell.KeyCtrlC || event.Rune() == 'q':
				cancel()
				a.tview.Stop()
				return nil

			// Navigation: ◄ ► cycles through all pages including logs.
			case event.Key() == tcell.KeyRight:
				navigateToPage((currentPage + 1) % len(dashPages))
				return nil
			case event.Key() == tcell.KeyLeft:
				navigateToPage((currentPage - 1 + len(dashPages)) % len(dashPages))
				return nil

			// Quick jump to logs page.
			case event.Key() == tcell.KeyF2 || event.Rune() == 'L':
				navigateToPage(logsPageIdx)
				return nil

			// Node toggle (works from any page).
			case event.Rune() == 'R':
				a.handleNodeToggle(ctx, pages, a.nodeMgr, nodeView.NodeControl, dashPages[currentPage])
				return nil
			}

			// --- Log viewer page-specific keys ---
			if currentFront == pageLogs {
				switch {
				case event.Key() == tcell.KeyEscape || event.Key() == tcell.KeyF1:
					switchToDashboard()
					return nil
				case event.Rune() == '/':
					logViewer.EnterSearchMode()
					a.tview.SetFocus(logViewer.SearchBar())
					return nil
				}
				// 1-4, Space, Up/Down handled by content's InputCapture.
			}

			return event
		}).Run(); err != nil {
		return err
	}
	return nil
}

// safeGo wraps a function with panic recovery, logging the stack to etui-crash.log
// and sending the error to errCh for display in the TUI footer.
func (a *App) safeGo(name string, errCh chan error, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("panic in %s: %v\n%s", name, r, debug.Stack())
			a.writeCrashLog(msg)
			a.log.Error("panic in %s: %v", name, r)
			select {
			case errCh <- fmt.Errorf("panic in %s: %v (see etui-crash.log)", name, r):
			default:
			}
		}
	}()
	fn()
}

func (a *App) writeCrashLog(msg string) {
	logPath := filepath.Join(a.datadir, "etui-crash.log")
	entry := fmt.Sprintf("[%s] %s\n\n", time.Now().Format(time.RFC3339), msg)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(entry)
}

// fillStagesInfo reads StagesInfo from the channel and updates the node-info view.
func (a *App) fillStagesInfo(ctx context.Context, view *widgets.NodeInfoView, infoCh <-chan *commands.StagesInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			metrics := a.syncTracker.Update(info)
			a.alertMgr.CheckSyncMetrics(metrics)
			currentStage := leadingStageName(info)
			a.tview.QueueUpdateDraw(func() {
				view.SyncStatus.UpdateSyncStatus(metrics, currentStage)
				view.Stages.Clear()
				view.Stages.SetText(info.Stages())
				view.DomainII.Clear()
				view.DomainII.SetText(info.DomainII())
			})
		}
	}
}

// leadingStageName returns the last stage in pipeline order that has non-zero
// progress. This is the furthest stage the sync has reached, giving a reliable
// indicator of current sync position regardless of early-sync zero values.
func leadingStageName(info *commands.StagesInfo) string {
	if len(info.StagesProgress) == 0 {
		return "—"
	}
	last := "—"
	for _, sp := range info.StagesProgress {
		if sp.Progress > 0 {
			last = string(sp.Stage)
		}
	}
	return last
}

// runClock updates the clock widget every second.
func (a *App) runClock(ctx context.Context, clock *tview.TextView) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			now := tick.Format("15:04:05")
			a.tview.QueueUpdateDraw(func() {
				clock.SetText(now)
			})
		}
	}
}

// handleErrors drains errors and displays them in the footer.
func (a *App) handleErrors(ctx context.Context, errCh <-chan error, view *tview.TextView) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errCh:
			if !ok {
				return
			}
			if err != nil {
				a.log.Error("errCh: %v", err)
				a.tview.QueueUpdateDraw(func() {
					view.SetDynamicColors(true)
					view.SetText(
						"[red::b]ERROR:[-] " + err.Error() + "   [::d](press q or Ctrl+C to quit)[-]",
					)
				})
			}
		}
	}
}

// pollDownloader polls the downloader API and renders progress with speed/ETA.
// Transient HTTP errors trigger exponential backoff (up to 30s) rather than
// a permanent exit, so the widget recovers once the downloader service starts.
func (a *App) pollDownloader(ctx context.Context, view *widgets.DownloaderView, errCh chan error) {
	const (
		maxBackoff = 30 * time.Second
		minBackoff = 500 * time.Millisecond
	)
	pollInterval := time.Duration(config.DownloaderPollInterval) * time.Millisecond
	backoff := minBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		res, err := a.dp.GetTorrentsInfo(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down
			}
			a.alertMgr.CheckDownloaderError(err)
			errMsg := err.Error()
			a.tview.QueueUpdateDraw(func() {
				view.SetError(errMsg)
			})
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Successful response — reset backoff and clear downloader alert
		backoff = minBackoff
		a.alertMgr.CheckDownloaderError(nil)

		stats := a.dlTracker.Update(res)
		a.tview.QueueUpdateDraw(func() {
			view.UpdateDownloader(stats)
		})

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

// pollSystemHealth periodically collects system metrics and updates the widget.
func (a *App) pollSystemHealth(ctx context.Context, view *widgets.SystemHealthView) {
	ticker := time.NewTicker(datasource.SystemPollInterval)
	defer ticker.Stop()

	// Collect once immediately so the widget isn't empty for 5 seconds
	stats := a.sysColl.CollectSystemStats()
	iops := a.iopsTrack.Update(stats.DiskIOPS_R, stats.DiskIOPS_W)
	a.alertMgr.CheckSystemStats(stats)
	a.tview.QueueUpdateDraw(func() {
		view.UpdateSystemHealth(stats, iops)
	})

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := a.sysColl.CollectSystemStats()
			iops := a.iopsTrack.Update(stats.DiskIOPS_R, stats.DiskIOPS_W)
			a.alertMgr.CheckSystemStats(stats)
			a.tview.QueueUpdateDraw(func() {
				view.UpdateSystemHealth(stats, iops)
			})
		}
	}
}

// pollAlerts periodically refreshes the alerts widget from the AlertManager.
func (a *App) pollAlerts(ctx context.Context, view *widgets.AlertsView) {
	const pollInterval = 1 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastVersion int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v := a.alertMgr.Version()
			if v == lastVersion {
				continue // no new alerts — skip redraw
			}
			lastVersion = v
			// The alerts panel is 5 rows with a border → 3 visible content lines.
			alerts := a.alertMgr.Recent(3)
			a.tview.QueueUpdateDraw(func() {
				view.UpdateAlerts(alerts)
			})
		}
	}
}

// pollLogTail periodically reads the Erigon log file and updates the widget.
// File I/O runs in this goroutine; only the cheap SetText call is queued
// onto the tview event loop.
func (a *App) pollLogTail(ctx context.Context, view *widgets.LogTailView) {
	const pollInterval = 500 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Read once immediately.
	text := view.ReadLogTail()
	a.tview.QueueUpdateDraw(func() {
		view.SetText(text)
		view.ScrollToEnd()
	})

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			text := view.ReadLogTail()
			a.tview.QueueUpdateDraw(func() {
				view.SetText(text)
				view.ScrollToEnd()
			})
		}
	}
}

// pollLogViewer periodically tails the log file via LogTailer and updates
// the full-screen log viewer widget.
func (a *App) pollLogViewer(ctx context.Context, viewer *widgets.LogViewerPage) {
	const pollInterval = 500 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastVersion int64

	// Initial render from seeded data.
	lines := a.logTailer.Recent(logRingSize, viewer.FilterLevel())
	a.tview.QueueUpdateDraw(func() {
		viewer.UpdateContent(lines)
	})
	lastVersion = a.logTailer.Version()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Poll for new file content (I/O happens here, not on event loop).
			a.logTailer.Poll()

			v := a.logTailer.Version()
			if v == lastVersion {
				continue
			}
			lastVersion = v

			lines := a.logTailer.Recent(logRingSize, viewer.FilterLevel())
			a.tview.QueueUpdateDraw(func() {
				viewer.UpdateContent(lines)
			})
		}
	}
}

// pollNodeStatus periodically checks the node state and updates the widget.
func (a *App) pollNodeStatus(ctx context.Context, view *widgets.NodeControlView) {
	const pollInterval = 2 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastVersion int64

	// Initial render.
	a.nodeMgr.DetectExternal()
	status := a.nodeMgr.Status()
	a.tview.QueueUpdateDraw(func() {
		view.UpdateNodeStatus(status)
	})
	lastVersion = a.nodeMgr.Version()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.nodeMgr.DetectExternal()

			v := a.nodeMgr.Version()
			if v == lastVersion {
				continue
			}
			lastVersion = v

			status := a.nodeMgr.Status()
			a.tview.QueueUpdateDraw(func() {
				view.UpdateNodeStatus(status)
			})
		}
	}
}

// logRingSize matches the datasource ring buffer size for the full viewer.
const logRingSize = 1000
