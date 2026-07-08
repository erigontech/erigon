package tui

import (
	"context"
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules"
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules/nodeinfo"
	"github.com/erigontech/erigon/cmd/etui/internals/tui/modules/startpage"
	"github.com/erigontech/erigon/cmd/integration/commands"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/schollz/progressbar/v3"
	"time"
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

	go t.FillStagesInfo(view, infoCh)
	go t.Clock(view.Clock)
	go t.HandleErrors(errCh, footer)

	t.FillDownloaderProgress(view.Downloader, errCh)

	currentPage, pagesCount := 0, 2
	names := []string{"start", "nodeInfo"}
	pages.AddPage(names[0], startPage, true, true)
	pages.AddPage(names[1], nodeInfo, true, false)

	if err := t.app.SetRoot(pages, true).EnableMouse(true).SetInputCapture(
		func(event *tcell.EventKey) *tcell.EventKey {
			switch {
			case event.Key() == tcell.KeyCtrlC || event.Rune() == 'q':
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

func (t *TUI) FillStagesInfo(body *nodeinfo.BodyView, infoCh <-chan *commands.StagesInfo) {
	for info := range infoCh {
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

func (t *TUI) Clock(clock *tview.TextView) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for tick := range ticker.C {
		now := tick.Format("15:04:05")
		t.app.QueueUpdateDraw(func() {
			clock.SetText(now)
		})
	}
}

func (t *TUI) HandleErrors(errCh <-chan error, view *tview.TextView) {
	if err, ok := <-errCh; ok && err != nil {
		t.app.QueueUpdateDraw(func() {
			view.SetDynamicColors(true)
			//view.SetText(
			//	"[red::b]ERROR:[-] " + err.Error() + "   [::d](press q or Ctrl+C to quit)[-]",
			//)
		})
		//time.AfterFunc(5*time.Second, app.Stop)
	}
}

func (t *TUI) FillDownloaderProgress(progressBar *tview.TextView, errCh chan error) {
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
			res, err := t.dp.GetTorrentsInfo(context.Background())
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			bar.ChangeMax(res.Total)
			bar.Set(res.Complete)
			time.Sleep(100 * time.Millisecond)
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
