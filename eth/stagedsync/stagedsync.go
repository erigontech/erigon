package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

const prof = false // whether to profile

type StagedSync struct {
	PrefetchedBlocks *PrefetchedBlocks
	stageBuilders    StageBuilders
	unwindOrder      UnwindOrder
}

func New(stages StageBuilders, unwindOrder UnwindOrder) *StagedSync {
	return &StagedSync{
		PrefetchedBlocks: NewPrefetchedBlocks(),
		stageBuilders:    stages,
		unwindOrder:      unwindOrder,
	}
}

func (stagedSync *StagedSync) Prepare(
	d DownloaderGlue,
	chainConfig *params.ChainConfig,
	chainContext core.ChainContext,
	vmConfig *vm.Config,
	db ethdb.Database,
	tx ethdb.Database,
	pid string,
	storageMode ethdb.StorageMode,
	datadir string,
	quitCh <-chan struct{},
	headersFetchers []func() error,
	txPool *core.TxPool,
	poolStart func() error,
	changeSetHook ChangeSetHook,
) (*State, error) {
	defer log.Info("Staged sync finished")

	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			d:                d,
			chainConfig:      chainConfig,
			chainContext:     chainContext,
			vmConfig:         vmConfig,
			db:               db,
			tx:               tx,
			pid:              pid,
			storageMode:      storageMode,
			datadir:          datadir,
			quitCh:           quitCh,
			headersFetchers:  headersFetchers,
			txPool:           txPool,
			poolStart:        poolStart,
			changeSetHook:    changeSetHook,
			prefetchedBlocks: stagedSync.PrefetchedBlocks,
		},
	)
	state := NewState(stages)

	state.unwindOrder = make([]*Stage, len(stagedSync.unwindOrder))

	for i, stageIndex := range stagedSync.unwindOrder {
		state.unwindOrder[i] = stages[stageIndex]
	}

	if err := state.LoadUnwindInfo(db); err != nil {
		return nil, err
	}
	return state, nil
}
