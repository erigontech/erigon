package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

const prof = false // whether to profile

type StagedSync struct {
	PrefetchedBlocks *PrefetchedBlocks
	stageBuilders    StageBuilders
	unwindOrder      UnwindOrder
	params           OptionalParameters
}

// OptionalParameters contains any non-necessary parateres you can specify to fine-tune
// and experiment on StagedSync.
type OptionalParameters struct {
	// StateReaderBuilder is a function that returns state reader for the block execution stage.
	// It can be used to add someting like bloom filters to figure out non-existing accounts and similar experiments.
	StateReaderBuilder StateReaderBuilder
}

func New(stages StageBuilders, unwindOrder UnwindOrder, params OptionalParameters) *StagedSync {
	return &StagedSync{
		PrefetchedBlocks: NewPrefetchedBlocks(),
		stageBuilders:    stages,
		unwindOrder:      unwindOrder,
		params:           params,
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
	hdd bool,
	quitCh <-chan struct{},
	headersFetchers []func() error,
	txPool *core.TxPool,
	poolStart func() error,
	changeSetHook ChangeSetHook,
) (*State, error) {
	var readerBuilder StateReaderBuilder
	if stagedSync.params.StateReaderBuilder != nil {
		readerBuilder = stagedSync.params.StateReaderBuilder
	} else {
		readerBuilder = func(getter ethdb.Getter) state.StateReader { return state.NewPlainStateReader(getter) }
	}

	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			d:                  d,
			chainConfig:        chainConfig,
			chainContext:       chainContext,
			vmConfig:           vmConfig,
			db:                 db,
			TX:                 tx,
			pid:                pid,
			storageMode:        storageMode,
			datadir:            datadir,
			QuitCh:             quitCh,
			headersFetchers:    headersFetchers,
			txPool:             txPool,
			poolStart:          poolStart,
			changeSetHook:      changeSetHook,
			hdd:                hdd,
			prefetchedBlocks:   stagedSync.PrefetchedBlocks,
			stateReaderBuilder: readerBuilder,
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
