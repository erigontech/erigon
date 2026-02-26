package commands

import (
	"fmt"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"strings"
	"time"
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
	ChainInfo        ChainInfo
}

type ChainInfo struct {
	ChainID   uint64
	ChainName string
}

func (info *StagesInfo) OverviewTUI() string {
	return fmt.Sprintf(
		"[yellow]Chain:[-] ID [cyan]%d[-]  Name [green]%s[-]\n"+
			"[yellow]Prune mode:[-] %s\n"+
			"[yellow]Blocks:[-] seg: [cyan]%d[-]  ind: [cyan]%d[-]\n"+
			"[yellow]Bor blocks:[-] seg: [cyan]%d[-]  ind: [cyan]%d[-]\n"+
			"[yellow]Last & state.history:[-] txnum: [cyan]%d[-], blocknum: [cyan]%d[-], steps: [magenta]%.2f[-]\n"+
			"[yellow]EthTxSequence:[-] [cyan]%d[-]\n"+
			"[yellow]In DB:[-] first header [cyan]%d[-], last header [cyan]%d[-],\n"+
			"first body [cyan]%d[-], last body [cyan]%d[-]\n"+
			"[yellow]Last update on[-] [::d]%s[-]",
		info.ChainInfo.ChainID, info.ChainInfo.ChainName,
		info.PruneDistance.String(),
		info.SnapshotInfo.SegMax, info.SnapshotInfo.IndMax,
		info.BorSnapshotInfo.SegMax, info.BorSnapshotInfo.IndMax,
		info.LastInfo.TxNum, info.LastInfo.BlockNum, info.LastInfo.IdxSteps,
		info.EthTxSequence,
		info.DB.FirstHeader, info.DB.LastHeader, info.DB.FirstBody, info.DB.LastBody,
		time.Now().Format("15:04:05"))
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
