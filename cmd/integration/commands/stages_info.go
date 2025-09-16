package commands

import (
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

type DomainIIProgress struct {
	HistoryStartFrom uint64
	Name             string
	TxNum            uint64
	Step             uint64
}
