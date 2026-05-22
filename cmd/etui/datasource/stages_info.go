package datasource

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// StageProgress is the lightweight stage snapshot rendered by etui.
type StageProgress struct {
	Stage    stages.SyncStage
	Progress uint64
	PrunedTo uint64
}

// DomainIIProgress holds domain/index progress rows for the secondary table.
type DomainIIProgress struct {
	HistoryStartFrom uint64
	Name             string
	TxNum            uint64
	Step             uint64
}

// StagesInfo is the lightweight sync payload consumed by the TUI.
type StagesInfo struct {
	StagesProgress   []StageProgress
	DomainIIProgress []DomainIIProgress
}

// Stages renders the stages panel in a fixed-width table.
func (info *StagesInfo) Stages() string {
	res := "Stages:\n" + fmt.Sprintf("%-15s %12s %12s\n", "stage_at", "progress", "prune_at")
	res += strings.Repeat("-", 43) + "\n"
	for _, s := range info.StagesProgress {
		res += fmt.Sprintf("%-15s %12d %12d\n", s.Stage, s.Progress, s.PrunedTo)
	}
	return res
}

// DomainII renders the domain/index progress panel.
func (info *StagesInfo) DomainII() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%-12s %18s %18s %18s\n",
		"domain or ii name", "historyStartFrom", "progress(txnum)", "progress(step)")
	b.WriteString(strings.Repeat("-", 70) + "\n")

	for _, d := range info.DomainIIProgress {
		history := "-"
		if d.HistoryStartFrom != 0 {
			history = fmt.Sprintf("%d", d.HistoryStartFrom)
		}
		txnum := "-"
		if d.TxNum != 0 {
			txnum = fmt.Sprintf("%d", d.TxNum)
		}
		step := "-"
		if d.Step != 0 {
			step = fmt.Sprintf("%d", d.Step)
		}

		fmt.Fprintf(&b, "%-12s %18s %18s %18s\n",
			d.Name, history, txnum, step)
	}

	return b.String()
}
