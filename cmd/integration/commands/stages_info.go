package commands

import (
	"fmt"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"strings"
)

type StageProgress struct {
	Stage    stages.SyncStage
	Progress uint64
	PrunedTo uint64
}

type Last struct {
	TxNum    uint64
	BlockNum uint64
	IdxSteps float64
}
type Snapshot struct {
	SegMax uint64
	IndMax uint64
}

type DB struct {
	FirstHeader uint64
	LastHeader  uint64
	FirstBody   uint64
	LastBody    uint64
}
type StagesInfo struct {
	StagesProgress   []StageProgress
	PruneDistance    prune.Mode
	SnapshotInfo     Snapshot
	BorSnapshotInfo  Snapshot
	LastInfo         Last
	EthTxSequence    uint64
	DB               DB
	DomainIIProgress []DomainIIProgress
}

func (info *StagesInfo) Overview() string {
	return fmt.Sprintf(
		"Prune mode: %s\nblocks: seg: %d ind: %d\nbor blocks: seg: %d ind: %d\ninfo about last & state.history: txnum: %d, blocknum: %d, steps: %.2f\nEthTxSequence: %d\nIn DB: first header %d, last header %d, first body %d, last body %d",
		info.PruneDistance.String(), info.SnapshotInfo.SegMax, info.SnapshotInfo.IndMax,
		info.BorSnapshotInfo.SegMax, info.BorSnapshotInfo.IndMax,
		info.LastInfo.TxNum, info.LastInfo.BlockNum, info.LastInfo.IdxSteps,
		info.EthTxSequence, info.DB.FirstHeader, info.DB.LastHeader, info.DB.FirstBody, info.DB.LastBody)
}

func (info *StagesInfo) Stages() string {
	res := "Stages:\n" + fmt.Sprintf("%-15s %12s %12s\n", "stage_at", "progress", "prune_at")
	res += strings.Repeat("-", 43) + "\n"
	for _, s := range info.StagesProgress {
		res += fmt.Sprintf("%-15s %12d %12d\n", s.Stage, s.Progress, s.PrunedTo)
	}
	return res
}

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

type DomainIIProgress struct {
	HistoryStartFrom uint64
	Name             string
	TxNum            uint64
	Step             uint64
}
