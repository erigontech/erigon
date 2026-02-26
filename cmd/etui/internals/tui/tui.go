package tui

import (
	"context"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/schollz/progressbar/v3"

	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules"
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules/nodeinfo"
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules/startpage"
	"github.com/erigontech/erigon/cmd/integration/commands"
)

type TUI struct {
	app     *tview.Application
	dp      *nodeinfo.DownloaderPinger
	datadir string
}

func NewTUI(datadir string) *TUI {
	return &TUI{
		datadir: datadir,
		app:     tview.NewApplication(),
		dp:      nodeinfo.NewDownloaderPinger("http://localhost:6060"),
	}
}

func (t *TUI) Run(infoCh <-chan *commands.StagesInfo, errCh chan error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pages := tview.NewPages()
	nodeInfoBody, view := nodeinfo.Body()
	footer := modules.Footer()
	startPageBody, startView := startpage.Body(view.Clock, t.datadir)
	_ = startView
	startPage := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(modules.Header(), 1, 1, false).
		AddItem(startPageBody, 0, 5, false).
		AddItem(footer, 2, 1, false)
	nodeInfo := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(modules.Header(), 1, 1, false).
		AddItem(nodeInfoBody, 0, 5, false).
		AddItem(footer, 2, 1, false)

	go t.FillStagesInfo(ctx, view, infoCh)
	go t.Clock(ctx, view.Clock)
	go t.HandleErrors(ctx, errCh, footer)

	t.FillDownloaderProgress(ctx, view.Downloader, errCh)

	currentPage, pagesCount := 0, 2
	names := []string{"start", "nodeInfo"}
	pages.AddPage(names[0], startPage, true, true)
	pages.AddPage(names[1], nodeInfo, true, false)

	if err := t.app.SetRoot(pages, true).EnableMouse(true).SetInputCapture(
		func(event *tcell.EventKey) *tcell.EventKey {
			switch {
			case event.Key() == tcell.KeyCtrlC || event.Rune() == 'q':
				cancel()
				t.app.Stop()
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

func (t *TUI) FillStagesInfo(ctx context.Context, body *nodeinfo.BodyView, infoCh <-chan *commands.StagesInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			t.app.QueueUpdateDraw(func() {
				body.Overview.Clear()
				body.Overview.SetText(info.OverviewTUI())
				body.Stages.Clear()
				body.Stages.SetText(info.Stages())
				body.DomainII.Clear()
				body.DomainII.SetText(info.DomainII())
			})
		}
	}
}

func (t *TUI) Clock(ctx context.Context, clock *tview.TextView) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			now := tick.Format("15:04:05")
			t.app.QueueUpdateDraw(func() {
				clock.SetText(now)
			})
		}
	}
}

func (t *TUI) HandleErrors(ctx context.Context, errCh <-chan error, view *tview.TextView) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errCh:
			if !ok {
				return
			}
			if err != nil {
				t.app.QueueUpdateDraw(func() {
					view.SetDynamicColors(true)
					view.SetText(
						"[red::b]ERROR:[-] " + err.Error() + "   [::d](press q or Ctrl+C to quit)[-]",
					)
				})
			}
		}
	}
}

func (t *TUI) FillDownloaderProgress(ctx context.Context, progressBar *tview.TextView, errCh chan error) {
	writer := &TviewWriter{
		app:  t.app,
		text: progressBar,
	}

	bar := progressbar.NewOptions(100,
		progressbar.OptionSetWriter(writer),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionSetWidth(30),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetDescription("Downloading"),
	)

	go func() {
		for !bar.IsFinished() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			res, err := t.dp.GetTorrentsInfo(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // shutting down, don't report
				}
				select {
				case errCh <- err:
				default:
				}
				return
			}
			bar.ChangeMax(res.Total)
			bar.Set(res.Complete) //nolint:errcheck
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
}

type TviewWriter struct {
	app  *tview.Application
	text *tview.TextView
}

func (w *TviewWriter) Write(p []byte) (int, error) {
	s := string(p)

	w.app.QueueUpdateDraw(func() {
		w.text.SetText(s)
	})
	return len(p), nil
}
