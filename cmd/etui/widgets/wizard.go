package widgets

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
)

// WizardStep identifies which page of the wizard is active.
type WizardStep int

const (
	WizardStepNetwork WizardStep = iota
	WizardStepDatadir
	WizardStepNodeType
	WizardStepConfirm
	wizardStepCount // sentinel — total number of steps
)

// InstallWizard guides the user through first-run configuration.
type InstallWizard struct {
	Root *tview.Pages

	pages    []*tview.Flex
	step     WizardStep
	cfg      config.TUIConfig
	onFinish func(config.TUIConfig) // called when the wizard completes
	onCancel func()                 // called when the user cancels

	// Widgets that need to be read back.
	networkList  *tview.List
	datadirInput *tview.InputField
	spaceLabel   *tview.TextView
	nodeTypeList *tview.List
	summaryText  *tview.TextView
}

// NewInstallWizard creates the 4-step install wizard.
// onFinish is called with the final config. onCancel is called if the user aborts.
func NewInstallWizard(defaultDatadir string, onFinish func(config.TUIConfig), onCancel func()) *InstallWizard {
	w := &InstallWizard{
		Root:     tview.NewPages(),
		cfg:      config.Defaults(),
		onFinish: onFinish,
		onCancel: onCancel,
	}
	w.cfg.DataDir = defaultDatadir

	w.buildNetworkStep()
	w.buildDatadirStep()
	w.buildNodeTypeStep()
	w.buildConfirmStep()

	for i, p := range w.pages {
		w.Root.AddPage(fmt.Sprintf("step-%d", i), p, true, i == 0)
	}

	return w
}

// Focusable returns the primitive that should receive focus for the current step.
func (w *InstallWizard) Focusable() tview.Primitive {
	switch w.step {
	case WizardStepNetwork:
		return w.networkList
	case WizardStepDatadir:
		return w.datadirInput
	case WizardStepNodeType:
		return w.nodeTypeList
	case WizardStepConfirm:
		return w.summaryText
	}
	return w.Root
}

func (w *InstallWizard) navigate(delta int) {
	next := int(w.step) + delta
	if next < 0 {
		return // already at first step
	}
	if next >= int(wizardStepCount) {
		return // already at last step
	}

	// If advancing to confirm step, refresh the summary.
	if WizardStep(next) == WizardStepConfirm {
		w.refreshSummary()
	}
	// If advancing to datadir step, refresh the space check.
	if WizardStep(next) == WizardStepDatadir {
		w.updateFreeSpace(w.datadirInput.GetText())
	}

	w.step = WizardStep(next)
	w.Root.SwitchToPage(fmt.Sprintf("step-%d", next))
}

// --- Step 1: Network Selection ---

func (w *InstallWizard) buildNetworkStep() {
	w.networkList = tview.NewList().
		AddItem("mainnet", "Ethereum mainnet", 'm', nil).
		AddItem("hoodi", "Hoodi testnet", 'h', nil).
		AddItem("sepolia", "Sepolia testnet", 's', nil)
	w.networkList.SetBorder(true).SetTitle(" Select Network ")
	w.networkList.SetCurrentItem(0)

	// Select on Enter.
	w.networkList.SetSelectedFunc(func(_ int, mainText string, _ string, _ rune) {
		w.cfg.Chain = mainText
		w.navigate(1)
	})

	page := w.wizardFrame("Step 1/4: Choose Network", w.networkList,
		"[yellow]Enter[-]=select  [yellow]Esc[-]=cancel")

	page.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			w.onCancel()
			return nil
		}
		return event
	})

	w.pages = append(w.pages, page)
}

// --- Step 2: Data Directory ---

func (w *InstallWizard) buildDatadirStep() {
	w.datadirInput = tview.NewInputField().
		SetLabel("Data directory: ").
		SetText(w.cfg.DataDir).
		SetFieldWidth(60).
		SetChangedFunc(func(text string) {
			w.updateFreeSpace(text)
		})

	w.spaceLabel = tview.NewTextView().SetDynamicColors(true).
		SetText("[::d]Checking free space...[-]")

	inner := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(w.datadirInput, 1, 0, true).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(w.spaceLabel, 3, 0, false)
	inner.SetBorder(true).SetTitle(" Data Directory ")

	page := w.wizardFrame("Step 2/4: Choose Data Directory", inner,
		"[yellow]Enter[-]=next  [yellow]Esc[-]=back")

	page.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			w.navigate(-1)
			return nil
		case tcell.KeyEnter:
			w.cfg.DataDir = w.datadirInput.GetText()
			w.navigate(1)
			return nil
		}
		return event
	})

	w.pages = append(w.pages, page)
}

func (w *InstallWizard) updateFreeSpace(dir string) {
	if dir == "" {
		w.spaceLabel.SetText("[red]No directory specified[-]")
		return
	}

	// Walk up until we find a directory that exists.
	checkDir := dir
	for {
		info, err := os.Stat(checkDir)
		if err == nil && info.IsDir() {
			break
		}
		parent := filepath.Dir(checkDir)
		if parent == checkDir {
			// Reached filesystem root without finding a valid dir.
			w.spaceLabel.SetText("[yellow]Cannot determine free space (path does not exist yet)[-]")
			return
		}
		checkDir = parent
	}

	freeGB, err := diskFreeGB(checkDir)
	if err != nil {
		w.spaceLabel.SetText(fmt.Sprintf("[yellow]Cannot check free space: %v[-]", err))
		return
	}

	color := "green"
	warning := ""
	if freeGB < 500 {
		color = "yellow"
		warning = "\n[yellow]Warning: Erigon mainnet needs ~2TB for a full node[-]"
	}
	if freeGB < 100 {
		color = "red"
		warning = "\n[red]Warning: Very low disk space! Erigon requires at least ~800GB[-]"
	}
	w.spaceLabel.SetText(fmt.Sprintf("[%s]Free space: %.1f GB[-]%s", color, freeGB, warning))
}

// --- Step 3: Node Type ---

func (w *InstallWizard) buildNodeTypeStep() {
	w.nodeTypeList = tview.NewList().
		AddItem("full", "Pruned node — keeps recent state (~2TB)", 'f', nil).
		AddItem("archive", "Full archive — all history (~3TB+)", 'a', nil).
		AddItem("minimal", "Minimal pruning — smallest storage (~800GB)", 'm', nil)
	w.nodeTypeList.SetBorder(true).SetTitle(" Select Node Type ")
	w.nodeTypeList.SetCurrentItem(0)

	w.nodeTypeList.SetSelectedFunc(func(_ int, mainText string, _ string, _ rune) {
		w.cfg.PruneMode = mainText
		w.navigate(1)
	})

	page := w.wizardFrame("Step 3/4: Choose Node Type", w.nodeTypeList,
		"[yellow]Enter[-]=select  [yellow]Esc[-]=back")

	page.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			w.navigate(-1)
			return nil
		}
		return event
	})

	w.pages = append(w.pages, page)
}

// --- Step 4: Confirmation ---

func (w *InstallWizard) buildConfirmStep() {
	w.summaryText = tview.NewTextView().SetDynamicColors(true)
	w.summaryText.SetBorder(true).SetTitle(" Confirm Configuration ")

	page := w.wizardFrame("Step 4/4: Review & Confirm", w.summaryText,
		"[yellow]Enter[-]=save & start  [yellow]Esc[-]=back")

	page.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape:
			w.navigate(-1)
			return nil
		case tcell.KeyEnter:
			w.onFinish(w.cfg)
			return nil
		}
		return event
	})

	w.pages = append(w.pages, page)
}

func (w *InstallWizard) refreshSummary() {
	text := fmt.Sprintf(
		`[green::b]Configuration Summary[-]

  [yellow]Network:[-]         %s
  [yellow]Data Directory:[-]  %s
  [yellow]Node Type:[-]       %s — %s
  [yellow]RPC:[-]             %s (port %d)
  [yellow]Private API:[-]     %s
  [yellow]Diagnostics:[-]     %s

  Config will be saved to: [cyan]%s[-]

  [green]Press Enter to save and start, or Esc to go back.[-]`,
		w.cfg.Chain,
		w.cfg.DataDir,
		w.cfg.PruneMode, config.PruneModeDescription(w.cfg.PruneMode),
		boolToEnabled(w.cfg.RPCEnabled), w.cfg.RPCPort,
		w.cfg.PrivateAPIAddr,
		w.cfg.DiagnosticsURL,
		config.ConfigPath(w.cfg.DataDir),
	)
	w.summaryText.SetText(text)
}

func boolToEnabled(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

// wizardFrame wraps a content primitive with the standard wizard chrome:
// header, step title, content, and a help bar.
func (w *InstallWizard) wizardFrame(title string, content tview.Primitive, helpText string) *tview.Flex {
	titleView := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter).
		SetText(fmt.Sprintf("[::b]Erigon Setup Wizard — %s[-]", title))

	helpBar := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter).
		SetText(helpText)

	return tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(Header(), 1, 0, false).
		AddItem(titleView, 1, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(content, 0, 1, true).
		AddItem(helpBar, 1, 0, false)
}
