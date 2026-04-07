package widgets

import (
	"fmt"
	"strings"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

type ValidatorPageView struct {
	Root        *tview.Pages
	empty       *tview.TextView
	duties      *tview.TextView
	performance *tview.TextView
	slashGuard  *tview.TextView
}

func NewValidatorPage() *ValidatorPageView {
	empty := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter).
		SetText("\n\n[yellow]No validator keys configured[-]")
	empty.SetBorder(true).SetTitle(" Validator ")

	duties := newValidatorPanel(" Duties ")
	performance := newValidatorPanel(" Performance (24h) ")
	slashGuard := newValidatorPanel(" Slash Guard ")

	content := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(duties, 5, 0, false).
		AddItem(performance, 4, 0, false).
		AddItem(slashGuard, 3, 0, false)

	pages := tview.NewPages().
		AddPage("empty", empty, true, true).
		AddPage("content", content, true, false)

	return &ValidatorPageView{
		Root:        pages,
		empty:       empty,
		duties:      duties,
		performance: performance,
		slashGuard:  slashGuard,
	}
}

func newValidatorPanel(title string) *tview.TextView {
	tv := tview.NewTextView().SetDynamicColors(true)
	tv.SetBorder(true).SetTitle(title)
	return tv
}

func (v *ValidatorPageView) SetSnapshot(snapshot datasource.ValidatorSnapshot) {
	if !snapshot.HasKeys {
		v.empty.SetText("\n\n[yellow]No validator keys configured[-]")
		v.Root.SwitchToPage("empty")
		return
	}

	v.Root.SwitchToPage("content")
	v.duties.SetText(formatValidatorDuties(snapshot))
	v.performance.SetText(formatValidatorPerformance(snapshot))
	v.slashGuard.SetText(formatValidatorSlashGuard(snapshot))
}

func formatValidatorDuties(snapshot datasource.ValidatorSnapshot) string {
	if snapshot.Duties.Error != "" {
		return fmt.Sprintf("[red]Beacon API unavailable[-]\n%s", tview.Escape(snapshot.Duties.Error))
	}
	if snapshot.ActiveValidatorCount == 0 {
		return "[yellow]No active validators found[-]"
	}

	return strings.Join([]string{
		formatDutyLine("Next attestation", snapshot.Duties.NextAttestation, snapshot.Duties.CurrentSlot, snapshot.Duties.SecondsPerSlot),
		formatDutyLine("Next proposer", snapshot.Duties.NextProposer, snapshot.Duties.CurrentSlot, snapshot.Duties.SecondsPerSlot),
	}, "\n")
}

func formatValidatorPerformance(snapshot datasource.ValidatorSnapshot) string {
	if snapshot.Performance.Error != "" {
		return fmt.Sprintf("[red]Performance unavailable[-]\n%s", tview.Escape(snapshot.Performance.Error))
	}
	if snapshot.ActiveValidatorCount == 0 {
		return "[yellow]No active validators found[-]"
	}

	return fmt.Sprintf(
		"[yellow]Attestations:[-] %.1f%% (missed: %d)\n[yellow]Inclusion delay:[-] p50=%d, p95=%d",
		snapshot.Performance.AttestationPct,
		snapshot.Performance.Missed,
		snapshot.Performance.InclusionP50,
		snapshot.Performance.InclusionP95,
	)
}

func formatValidatorSlashGuard(snapshot datasource.ValidatorSnapshot) string {
	color := "green"
	if snapshot.SlashGuard == datasource.SlashGuardWarning {
		color = "yellow"
	}

	msg := fmt.Sprintf("[%s::b]%s[-]", color, snapshot.SlashGuard)
	if snapshot.ActiveValidatorCount == 0 {
		msg += "\n[yellow]No active validators available[-]"
	}
	return msg
}

func formatDutyLine(label string, duty *datasource.ValidatorDuty, currentSlot, secondsPerSlot uint64) string {
	if duty == nil {
		return fmt.Sprintf("[yellow]%-18s[-] [::d]—[-]", label+":")
	}

	return fmt.Sprintf(
		"[yellow]%-18s[-] slot [cyan]%d[-]  [green]%s[-]",
		label+":",
		duty.Slot,
		formatDutyETA(duty.Slot, currentSlot, secondsPerSlot),
	)
}

func formatDutyETA(slot, currentSlot, secondsPerSlot uint64) string {
	if slot <= currentSlot {
		return "+00:00"
	}
	seconds := int64(slot-currentSlot) * int64(secondsPerSlot)
	minutes := seconds / 60
	secs := seconds % 60
	if minutes < 60 {
		return fmt.Sprintf("+%02d:%02d", minutes, secs)
	}

	hours := minutes / 60
	return fmt.Sprintf("+%02d:%02d:%02d", hours, minutes%60, secs)
}
