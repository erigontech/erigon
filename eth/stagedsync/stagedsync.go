package stagedsync

import (
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
)

type StagedSync struct {
	PrefetchedBlocks *bodydownload.PrefetchedBlocks
	stageBuilders    StageBuilders
	unwindOrder      UnwindOrder
	Notifier         ChainEventNotifier
	params           OptionalParameters
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

	SnapshotDir      string
	TorrentClient    *snapshotsync.Client
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
	vmConfig *vm.Config,
	db ethdb.Database,
	tx ethdb.Tx,
	storageMode ethdb.StorageMode,
	quitCh <-chan struct{},
	initialCycle bool,
	miningConfig *MiningCfg,
	accumulator *shards.Accumulator,
) (*State, error) {

	if vmConfig == nil {
		vmConfig = &vm.Config{}
	}
	vmConfig.EnableTEMV = storageMode.TEVM

	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			DB:              db,
			QuitCh:          quitCh,
			InitialCycle:    initialCycle,
			mining:          miningConfig,
			snapshotsDir:    stagedSync.params.SnapshotDir,
			btClient:        stagedSync.params.TorrentClient,
			SnapshotBuilder: stagedSync.params.SnapshotMigrator,
			Accumulator:     accumulator,
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
	stagedSync.params.TorrentClient = client
	stagedSync.params.SnapshotDir = snapshotsDir
	stagedSync.params.SnapshotMigrator = snapshotMigrator
}
func (stagedSync *StagedSync) GetSnapshotMigratorFinal() func(tx ethdb.Tx) error {
	if stagedSync.params.SnapshotMigrator != nil {
		return stagedSync.params.SnapshotMigrator.Final
	}
	return nil
}
