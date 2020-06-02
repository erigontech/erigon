package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/params"
)

type BlockChain interface {
	core.ChainContext
	Config() *params.ChainConfig
	GetVMConfig() *vm.Config
	GetBlockByNumber(uint64) *types.Block
}

type DownloaderGlue interface {
	SpawnSync([]func() error) error
	SpawnBodyDownloadStage(string, uint64) (bool, error)
}
