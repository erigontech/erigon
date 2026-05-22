package widgets

import (
	"fmt"
	"strconv"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/config"
)

// ConfigureModal is a full-screen modal dialog for editing TUI configuration.
type ConfigureModal struct {
	// Root is the outermost primitive to embed in tview.Pages.
	Root *tview.Flex

	form    *tview.Form
	cfg     config.TUIConfig
	onSave  func(config.TUIConfig) // called when user confirms
	onClose func()                 // called when user cancels / presses Escape
}

// NewConfigureModal creates the configuration dialog pre-populated with cfg.
// onSave is called with the updated config when the user presses Save.
// onClose is called when the user cancels.
func NewConfigureModal(cfg config.TUIConfig, onSave func(config.TUIConfig), onClose func()) *ConfigureModal {
	cm := &ConfigureModal{
		cfg:     cfg,
		onSave:  onSave,
		onClose: onClose,
	}

	cm.form = tview.NewForm()
	cm.form.SetBorder(true).SetTitle(" Configuration ").SetTitleAlign(tview.AlignCenter)

	// --- Network / Chain ---
	chains := config.ValidChains()
	chainIdx := 0
	for i, c := range chains {
		if c == cfg.Chain {
			chainIdx = i
			break
		}
	}
	cm.form.AddDropDown("Network", chains, chainIdx, func(_ string, _ int) {})

	// --- Data Directory ---
	cm.form.AddInputField("Data Directory", cfg.DataDir, 60, nil, func(_ string) {})

	// --- Prune Mode ---
	modes := config.ValidPruneModes()
	modeIdx := 0
	for i, m := range modes {
		if m == cfg.PruneMode {
			modeIdx = i
			break
		}
	}
	modeLabels := make([]string, len(modes))
	for i, m := range modes {
		modeLabels[i] = fmt.Sprintf("%s — %s", m, config.PruneModeDescription(m))
	}
	cm.form.AddDropDown("Prune Mode", modeLabels, modeIdx, func(_ string, _ int) {})

	// --- RPC Enabled ---
	cm.form.AddCheckbox("Enable HTTP RPC", cfg.RPCEnabled, func(_ bool) {})

	// --- RPC Port ---
	cm.form.AddInputField("RPC Port", config.FormatRPCPort(cfg.RPCPort), 10,
		// Accept only digits.
		func(text string, _ rune) bool {
			_, err := strconv.Atoi(text)
			return err == nil || text == ""
		}, func(_ string) {})

	// --- Private API Address ---
	cm.form.AddInputField("Private API Address", cfg.PrivateAPIAddr, 30, nil, func(_ string) {})

	// --- Diagnostics URL ---
	cm.form.AddInputField("Diagnostics URL", cfg.DiagnosticsURL, 50, nil, func(_ string) {})

	// --- Buttons ---
	cm.form.AddButton("Save", cm.handleSave)
	cm.form.AddButton("Cancel", cm.handleCancel)

	cm.form.SetCancelFunc(cm.handleCancel)

	// Center the form in a box.
	cm.Root = tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(cm.form, 22, 0, true).
			AddItem(nil, 0, 1, false),
			80, 0, true).
		AddItem(nil, 0, 1, false)

	// Capture Escape from the flex root as well.
	cm.Root.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			cm.handleCancel()
			return nil
		}
		return event
	})

	return cm
}

// Form returns the inner form for focus management.
func (cm *ConfigureModal) Form() *tview.Form {
	return cm.form
}

func (cm *ConfigureModal) handleSave() {
	// Read back all field values.
	_, cm.cfg.Chain = cm.form.GetFormItemByLabel("Network").(*tview.DropDown).GetCurrentOption()

	cm.cfg.DataDir = cm.form.GetFormItemByLabel("Data Directory").(*tview.InputField).GetText()

	// Parse prune mode from the composite label.
	_, modeLabel := cm.form.GetFormItemByLabel("Prune Mode").(*tview.DropDown).GetCurrentOption()
	modes := config.ValidPruneModes()
	modeLabels := make([]string, len(modes))
	for i, m := range modes {
		modeLabels[i] = fmt.Sprintf("%s — %s", m, config.PruneModeDescription(m))
	}
	for i, ml := range modeLabels {
		if ml == modeLabel {
			cm.cfg.PruneMode = modes[i]
			break
		}
	}

	cm.cfg.RPCEnabled = cm.form.GetFormItemByLabel("Enable HTTP RPC").(*tview.Checkbox).IsChecked()

	portStr := cm.form.GetFormItemByLabel("RPC Port").(*tview.InputField).GetText()
	if p, err := strconv.Atoi(portStr); err == nil {
		cm.cfg.RPCPort = p
	}

	cm.cfg.PrivateAPIAddr = cm.form.GetFormItemByLabel("Private API Address").(*tview.InputField).GetText()
	cm.cfg.DiagnosticsURL = cm.form.GetFormItemByLabel("Diagnostics URL").(*tview.InputField).GetText()

	if cm.onSave != nil {
		cm.onSave(cm.cfg)
	}
}

func (cm *ConfigureModal) handleCancel() {
	if cm.onClose != nil {
		cm.onClose()
	}
}
