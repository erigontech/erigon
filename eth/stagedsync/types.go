package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
)

type BlockChain interface {
	core.ChainContext
	GetVMConfig() *vm.Config
	GetBlockByNumber(uint64) *types.Block
}

type DownloaderGlue interface {
	SpawnHeaderDownloadStage([]func() error, *StageState, Unwinder) error
	SpawnBodyDownloadStage(string, *StageState, Unwinder) (bool, error)
}
