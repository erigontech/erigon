package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/ethdb"
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
	db ethdb.RwKV,
	tx ethdb.Tx,
	quitCh <-chan struct{},
	initialCycle bool,
) (*State, error) {
	stages := stagedSync.stageBuilders.Build(
		StageParameters{
			QuitCh:       quitCh,
			InitialCycle: initialCycle,
			snapshotsDir: stagedSync.params.SnapshotDir,
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
		if err := db.View(context.Background(), func(tx ethdb.Tx) error {
			return state.LoadUnwindInfo(tx)
		}); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (stagedSync *StagedSync) GetSnapshotMigratorFinal() func(tx ethdb.Tx) error {
	if stagedSync.params.SnapshotMigrator != nil {
		return stagedSync.params.SnapshotMigrator.Final
	}
	return nil
}
