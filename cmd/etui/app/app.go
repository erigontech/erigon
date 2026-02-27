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
	tview     *tview.Application
	dp        *datasource.DownloaderPinger
	sysColl   *datasource.SystemCollector
	iopsTrack *datasource.DiskIOPSTracker
	datadir   string
}

// New creates an App that reads from the given datadir.
func New(datadir string) *App {
	return &App{
		datadir:   datadir,
		tview:     tview.NewApplication(),
		dp:        datasource.NewDownloaderPinger(config.DefaultDownloaderURL),
		sysColl:   datasource.NewSystemCollector(datadir),
		iopsTrack: datasource.NewDiskIOPSTracker(),
	}
}

// Run starts the TUI event loop. It blocks until the user quits or the parent
// context is cancelled (e.g. by an OS signal).
func (a *App) Run(parent context.Context, infoCh <-chan *commands.StagesInfo, errCh chan error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Build pages
	pages := tview.NewPages()

	nodeInfoBody, nodeView := widgets.NewNodeInfoPage()
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

	// Start background goroutines
	go a.safeGo("fillStagesInfo", errCh, func() { a.fillStagesInfo(ctx, nodeView, infoCh) })
	go a.safeGo("runClock", errCh, func() { a.runClock(ctx, nodeView.Clock) })
	go a.handleErrors(ctx, errCh, footer) // not wrapped — it drains errCh itself
	go a.safeGo("pollDownloader", errCh, func() { a.pollDownloader(ctx, nodeView.Downloader, errCh) })
	go a.safeGo("pollSystemHealth", errCh, func() { a.pollSystemHealth(ctx, nodeView.SystemHealth) })

	// Page navigation
	currentPage, pagesCount := 0, 2
	names := []string{"start", "nodeInfo"}
	pages.AddPage(names[0], startPage, true, true)
	pages.AddPage(names[1], nodeInfoPage, true, false)

	if err := a.tview.SetRoot(pages, true).EnableMouse(true).SetInputCapture(
		func(event *tcell.EventKey) *tcell.EventKey {
			switch {
			case event.Key() == tcell.KeyCtrlC || event.Rune() == 'q':
				cancel()
				a.tview.Stop()
			case event.Key() == tcell.KeyRight:
				currentPage = (currentPage + 1 + pagesCount) % pagesCount
				pages.SwitchToPage(names[currentPage])
			case event.Key() == tcell.KeyLeft:
				currentPage = (currentPage - 1 + pagesCount) % pagesCount
				pages.SwitchToPage(names[currentPage])
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
			a.tview.QueueUpdateDraw(func() {
				view.Overview.Clear()
				view.Overview.SetText(info.OverviewTUI())
				view.Stages.Clear()
				view.Stages.SetText(info.Stages())
				view.DomainII.Clear()
				view.DomainII.SetText(info.DomainII())
			})
		}
	}
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

// pollDownloader polls the downloader API and renders progress as a text bar
// directly in a tview.TextView, replacing the progressbar/v3 dependency.
// Transient HTTP errors trigger exponential backoff (up to 30s) rather than
// a permanent exit, so the widget recovers once the downloader service starts.
func (a *App) pollDownloader(ctx context.Context, view *tview.TextView, errCh chan error) {
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
			// Show the error in the widget and retry with backoff
			errMsg := err.Error()
			a.tview.QueueUpdateDraw(func() {
				view.SetDynamicColors(true)
				view.SetText(fmt.Sprintf("[yellow]Downloader:[-] retrying... (%s)", errMsg))
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

		// Successful response — reset backoff
		backoff = minBackoff

		text := formatDownloadProgress(res.Complete, res.Total, res.Phase)
		a.tview.QueueUpdateDraw(func() {
			view.SetDynamicColors(true)
			view.SetText(text)
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
			a.tview.QueueUpdateDraw(func() {
				view.UpdateSystemHealth(stats, iops)
			})
		}
	}
}

// formatDownloadProgress renders a simple text-based progress bar.
func formatDownloadProgress(complete, total int, phase string) string {
	if total <= 0 {
		return "Downloading: waiting for torrents..."
	}

	if complete >= total {
		label := "Download complete"
		if phase != "" {
			label = fmt.Sprintf("[green]%s[-]: complete", phase)
		}
		return label
	}

	pct := float64(complete) / float64(total) * 100
	const barWidth = 30
	filled := int(float64(barWidth) * float64(complete) / float64(total))
	if filled > barWidth {
		filled = barWidth
	}
	bar := ""
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar += "█"
		} else {
			bar += "░"
		}
	}
	label := "Downloading"
	if phase != "" {
		label = phase
	}
	return fmt.Sprintf("[yellow]%s[-] %s [cyan]%d/%d[-] (%.1f%%)", label, bar, complete, total, pct)
}
