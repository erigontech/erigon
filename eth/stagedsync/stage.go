package stagedsync

import "github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"

type ExecFunc func(*StageState) error

type Stage struct {
	ID          stages.SyncStage
	Description string
	ExecFunc    ExecFunc
}
