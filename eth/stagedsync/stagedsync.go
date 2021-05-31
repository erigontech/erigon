package stagedsync

import (
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
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

	SilkwormExecutionFunc unsafe.Pointer

	SnapshotDir      string
	TorrnetClient    *snapshotsync.Client
	SnapshotMigrator *snapshotsync.SnapshotMigrator
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
	engine consensus.Engine,
	vmConfig *vm.Config,
	db ethdb.Database,
	tx ethdb.Tx,
	pid string,
	storageMode ethdb.StorageMode,
	tmpdir string,
	batchSize datasize.ByteSize,
	quitCh <-chan struct{},
	headersFetchers []func() error,
	txPool *core.TxPool,
	initialCycle bool,
	miningConfig *MiningCfg,
	accumulator *shards.Accumulator,
) (*State, error) {
	var readerBuilder StateReaderBuilder
	if stagedSync.params.StateReaderBuilder != nil {
		readerBuilder = stagedSync.params.StateReaderBuilder
	}

	var writerBuilder StateWriterBuilder
	if stagedSync.params.StateWriterBuilder != nil {
		writerBuilder = stagedSync.params.StateWriterBuilder
	}

	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			d:                     d,
			ChainConfig:           chainConfig,
			Engine:                engine,
			vmConfig:              vmConfig,
			DB:                    db,
			pid:                   pid,
			storageMode:           storageMode,
			TmpDir:                tmpdir,
			QuitCh:                quitCh,
			headersFetchers:       headersFetchers,
			txPool:                txPool,
			BatchSize:             batchSize,
			prefetchedBlocks:      stagedSync.PrefetchedBlocks,
			stateReaderBuilder:    readerBuilder,
			stateWriterBuilder:    writerBuilder,
			notifier:              stagedSync.Notifier,
			silkwormExecutionFunc: stagedSync.params.SilkwormExecutionFunc,
			InitialCycle:          initialCycle,
			mining:                miningConfig,
			snapshotsDir:          stagedSync.params.SnapshotDir,
			btClient:              stagedSync.params.TorrnetClient,
			SnapshotBuilder:       stagedSync.params.SnapshotMigrator,
			Accumulator:           accumulator,
		},
	)
	state := NewState(stages)

	state.unwindOrder = make([]*Stage, len(stagedSync.unwindOrder))

	for i, stageIndex := range stagedSync.unwindOrder {
		state.unwindOrder[i] = stages[stageIndex]
	}

	if tx != nil {
		if err := state.LoadUnwindInfo(tx); err != nil {
			return nil, err
		}
	} else {
		if err := state.LoadUnwindInfo(db); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (stagedSync *StagedSync) SetTorrentParams(client *snapshotsync.Client, snapshotsDir string, snapshotMigrator *snapshotsync.SnapshotMigrator) {
	stagedSync.params.TorrnetClient = client
	stagedSync.params.SnapshotDir = snapshotsDir
	stagedSync.params.SnapshotMigrator = snapshotMigrator
}
func (stagedSync *StagedSync) GetSnapshotMigratorFinal() func(tx ethdb.Tx) error {
	if stagedSync.params.SnapshotMigrator != nil {
		return stagedSync.params.SnapshotMigrator.Final
	}
	return nil
}
