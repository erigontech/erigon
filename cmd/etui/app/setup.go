package app

import (
	"context"
	"errors"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
	"github.com/erigontech/erigon/cmd/etui/datasource"
	"github.com/erigontech/erigon/cmd/etui/widgets"
)

// ErrSetupCancelled reports that the first-run setup was aborted by the user.
var ErrSetupCancelled = errors.New("setup cancelled")

type setupResult struct {
	cfg       config.TUIConfig
	cancelled bool
}

// RunSetup runs the install wizard as a dedicated setup-only application.
// It persists the selected config and returns the final datadir to monitor.
func RunSetup(parent context.Context, datadir string, opts Options) (string, error) {
	cfg, err := config.Load(datadir)
	if err != nil {
		cfg = config.Defaults()
		cfg.DataDir = datadir
	}
	if opts.Chain != "" {
		cfg.Chain = strings.ToLower(opts.Chain)
	}
	if opts.DiagnosticsURL != "" {
		cfg.DiagnosticsURL = opts.DiagnosticsURL
	}

	resultCh := make(chan setupResult, 1)
	tui := tview.NewApplication()
	wizard := widgets.NewInstallWizardWithConfig(cfg,
		func(newCfg config.TUIConfig) error {
			if err := newCfg.Validate(); err != nil {
				return err
			}
			if err := newCfg.SaveAll(); err != nil {
				return err
			}

			nodeMgr := datasource.NewNodeManager(newCfg.DataDir, newCfg.Chain)
			nodeMgr.Detect()
			status := nodeMgr.Status()
			if status.State != datasource.NodeRunning &&
				status.State != datasource.NodeStarting &&
				status.State != datasource.NodeStopping {
				if err := nodeMgr.Start(); err != nil {
					return err
				}
			}

			select {
			case resultCh <- setupResult{cfg: newCfg}:
			default:
			}
			tui.Stop()
			return nil
		},
		func() {
			select {
			case resultCh <- setupResult{cancelled: true}:
			default:
			}
			tui.Stop()
		},
	)

	go func() {
		<-parent.Done()
		tui.Stop()
	}()

	tui.SetRoot(wizard.Root, true).
		EnableMouse(true).
		SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			if event.Key() == tcell.KeyCtrlC {
				select {
				case resultCh <- setupResult{cancelled: true}:
				default:
				}
				tui.Stop()
				return nil
			}
			return event
		})
	tui.SetFocus(wizard.Focusable())

	if err := tui.Run(); err != nil {
		return "", err
	}

	select {
	case <-parent.Done():
		return "", parent.Err()
	case res := <-resultCh:
		if res.cancelled {
			return "", ErrSetupCancelled
		}
		return res.cfg.DataDir, nil
	default:
		if parent.Err() != nil {
			return "", parent.Err()
		}
		return "", ErrSetupCancelled
	}
}
