package stagedsync

import (
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
)

type StagedSync struct {
	PrefetchedBlocks *bodydownload.PrefetchedBlocks
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
		PrefetchedBlocks: bodydownload.NewPrefetchedBlocks(),
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
	cache *shards.StateCache,
	batchSize datasize.ByteSize,
	quitCh <-chan struct{},
	headersFetchers []func() error,
	txPool *core.TxPool,
	poolStart func() error,
	initialCycle bool,
	miningConfig *MiningStagesParameters,
) (*State, error) {
	var readerBuilder StateReaderBuilder
	if stagedSync.params.StateReaderBuilder != nil {
		readerBuilder = stagedSync.params.StateReaderBuilder
	}

	var writerBuilder StateWriterBuilder
	if stagedSync.params.StateWriterBuilder != nil {
		writerBuilder = stagedSync.params.StateWriterBuilder
	}

	if stagedSync.params.Notifier != nil {
		stagedSync.Notifier = stagedSync.params.Notifier
	}

	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			d:                     d,
			ChainConfig:           chainConfig,
			chainContext:          chainContext,
			vmConfig:              vmConfig,
			DB:                    db,
			TX:                    tx,
			pid:                   pid,
			storageMode:           storageMode,
			TmpDir:                tmpdir,
			QuitCh:                quitCh,
			headersFetchers:       headersFetchers,
			txPool:                txPool,
			poolStart:             poolStart,
			cache:                 cache,
			BatchSize:             batchSize,
			prefetchedBlocks:      stagedSync.PrefetchedBlocks,
			stateReaderBuilder:    readerBuilder,
			stateWriterBuilder:    writerBuilder,
			notifier:              stagedSync.Notifier,
			silkwormExecutionFunc: stagedSync.params.SilkwormExecutionFunc,
			InitialCycle:          initialCycle,
			mining:                miningConfig,
		},
	)
	state := NewState(stages)

	state.unwindOrder = make([]*Stage, len(stagedSync.unwindOrder))

	for i, stageIndex := range stagedSync.unwindOrder {
		state.unwindOrder[i] = stages[stageIndex]
	}

	if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		db = tx
	}
	if err := state.LoadUnwindInfo(db); err != nil {
		return nil, err
	}
	return state, nil
}
