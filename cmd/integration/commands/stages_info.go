package commands

import (
	"fmt"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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

type DomainIIProgress struct {
	HistoryStartFrom uint64
	Name             string
	TxNum            uint64
	Step             uint64
}
