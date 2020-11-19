package stagedsync

import (
	"unsafe"

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
	Notifier         ChainEventNotifier
}

// OptionalParameters contains any non-necessary parateres you can specify to fine-tune
// and experiment on StagedSync.
type OptionalParameters struct {
	// StateReaderBuilder is a function that returns state reader for the block execution stage.
	// It can be used to add someting like bloom filters to figure out non-existing accounts and similar experiments.
	StateReaderBuilder StateReaderBuilder

	// StateReaderBuilder is a function that returns state writer for the block execution stage.
	// It can be used to update bloom or other types of filters between block execution.
	StateWriterBuilder StateWriterBuilder

	// Notifier allows sending some data when new headers or new blocks are added
	Notifier ChainEventNotifier

	SilkwormExecutionFunc unsafe.Pointer
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
	chainContext *core.TinyChainContext,
	vmConfig *vm.Config,
	db ethdb.Database,
	tx ethdb.Database,
	pid string,
	storageMode ethdb.StorageMode,
	tmpdir string,
	batchSize int,
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

	var writerBuilder StateWriterBuilder
	if stagedSync.params.StateWriterBuilder != nil {
		writerBuilder = stagedSync.params.StateWriterBuilder
	} else {
		writerBuilder = func(db ethdb.Database, changeSetsDB ethdb.Database, blockNumber uint64) state.WriterWithChangeSets {
			return state.NewPlainStateWriter(db, changeSetsDB, blockNumber)
		}
	}

	if stagedSync.params.Notifier != nil {
		stagedSync.Notifier = stagedSync.params.Notifier
	}

	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			d:                     d,
			chainConfig:           chainConfig,
			chainContext:          chainContext,
			vmConfig:              vmConfig,
			db:                    db,
			TX:                    tx,
			pid:                   pid,
			storageMode:           storageMode,
			tmpdir:                tmpdir,
			QuitCh:                quitCh,
			headersFetchers:       headersFetchers,
			txPool:                txPool,
			poolStart:             poolStart,
			changeSetHook:         changeSetHook,
			batchSize:             batchSize,
			prefetchedBlocks:      stagedSync.PrefetchedBlocks,
			stateReaderBuilder:    readerBuilder,
			stateWriterBuilder:    writerBuilder,
			notifier:              stagedSync.Notifier,
			silkwormExecutionFunc: stagedSync.params.SilkwormExecutionFunc,
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
