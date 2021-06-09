package stagedsync

import (
	"fmt"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
)

type ChainEventNotifier interface {
	OnNewHeader(*types.Header)
	OnNewPendingLogs(types.Logs)
	OnNewPendingBlock(*types.Block)
	OnNewPendingTxs([]types.Transaction)
}

// StageParameters contains the stage that stages receives at runtime when initializes.
// Then the stage can use it to receive different useful functions.
type StageParameters struct {
	d           DownloaderGlue
	ChainConfig *params.ChainConfig
	vmConfig    *vm.Config
	Engine      consensus.Engine
	DB          ethdb.Database
	// TX is a current transaction that staged sync runs in. It contains all the latest changes that DB has.
	// It can be used for both reading and writing.
	pid         string
	BatchSize   datasize.ByteSize // Batch size for the execution stage
	storageMode ethdb.StorageMode
	TmpDir      string
	// QuitCh is a channel that is closed. This channel is useful to listen to when
	// the stage can take significant time and gracefully shutdown at Ctrl+C.
	QuitCh             <-chan struct{}
	headersFetchers    []func() error
	txPool             *core.TxPool
	prefetchedBlocks   *bodydownload.PrefetchedBlocks
	stateReaderBuilder StateReaderBuilder
	stateWriterBuilder StateWriterBuilder
	notifier           ChainEventNotifier
	InitialCycle       bool
	mining             *MiningCfg

	snapshotsDir    string
	btClient        *snapshotsync.Client
	SnapshotBuilder *snapshotsync.SnapshotMigrator
	Accumulator     *shards.Accumulator // State change accumulator
}

type MiningCfg struct {
	// noempty is the flag used to control whether the feature of pre-seal empty
	// block is enabled. The default value is false(pre-seal is enabled by default).
	// But in some special scenario the consensus engine will seal blocks instantaneously,
	// in this case this feature will add all empty blocks into canonical chain
	// non-stop and no real transaction will be included.
	noempty bool

	// runtime dat
	Block *miningBlock
}

func StageMiningCfg(noempty bool) *MiningCfg {
	return &MiningCfg{noempty: noempty, Block: &miningBlock{}}
}

// StageBuilder represent an object to create a single stage for staged sync
type StageBuilder struct {
	// ID is the stage identifier. Should be unique. It is recommended to prefix it with reverse domain `com.example.my-stage` to avoid conflicts.
	ID stages.SyncStage
	// Build is a factory function that initializes the sync stage based on the `StageParameters` provided.
	Build func(StageParameters) *Stage
}

// StageBuilders represents an ordered list of builders to build different stages. It also contains helper methods to change the list of stages.
type StageBuilders []StageBuilder

// MustReplace finds a stage with a specific ID and then sets the new one instead of that.
// Chainable but panics if it can't find stage to replace.
func (bb StageBuilders) MustReplace(id stages.SyncStage, newBuilder StageBuilder) StageBuilders {
	result := make([]StageBuilder, len(bb))

	found := false

	for i, originalBuilder := range bb {
		if strings.EqualFold(string(originalBuilder.ID), string(id)) {
			found = true
			result[i] = newBuilder
		} else {
			result[i] = originalBuilder
		}
	}

	if !found {
		panic(fmt.Sprintf("StageBuilders#Replace can't find the stage with id %s", string(id)))
	}

	return result
}

// Build creates sync states out of builders
func (bb StageBuilders) Build(world StageParameters) []*Stage {
	stages := make([]*Stage, len(bb))
	for i, builder := range bb {
		stages[i] = builder.Build(world)
	}
	return stages
}

func MiningStages(
	createBlockCfg MiningCreateBlockCfg,
	execCfg MiningExecCfg,
	hashStateCfg HashStateCfg,
	trieCfg TrieCfg,
	finish MiningFinishCfg,
) StageBuilders {
	return []StageBuilder{
		{
			ID: stages.MiningCreateBlock,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.MiningCreateBlock,
					Description: "Mining: construct new block from tx pool",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnMiningCreateBlockStage(s, tx,
							createBlockCfg,
							world.mining.Block,
							world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
				}
			},
		},
		{
			ID: stages.MiningExecution,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.MiningExecution,
					Description: "Mining: construct new block from tx pool",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnMiningExecStage(s, tx,
							execCfg,
							world.mining.Block,
							world.mining.Block.LocalTxs,
							world.mining.Block.RemoteTxs,
							world.mining.noempty,
							world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
				}
			},
		},
		{
			ID: stages.HashState,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.HashState,
					Description: "Hash the key in the state",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnHashStateStage(s, tx, hashStateCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
				}
			},
		},
		{
			ID: stages.IntermediateHashes,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.IntermediateHashes,
					Description: "Generate intermediate hashes and computing state root",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						stateRoot, err := SpawnIntermediateHashesStage(s, u, tx, trieCfg, world.QuitCh)
						if err != nil {
							return err
						}
						world.mining.Block.Header.Root = stateRoot
						return nil
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
				}
			},
		},
		{
			ID: stages.MiningFinish,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.MiningFinish,
					Description: "Mining: create and propagate valid block",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnMiningFinishStage(s, tx, world.mining.Block, finish, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error { return nil },
				}
			},
		},
	}
}

// UnwindOrder represents the order in which the stages needs to be unwound.
// Currently it is using indexes of stages, 0-based.
// The unwind order is important and not always just stages going backwards.
// Let's say, there is tx pool (state 10) can be unwound only after execution
// is fully unwound (stages 9...3).
type UnwindOrder []int

func MiningUnwindOrder() UnwindOrder {
	return []int{0, 1, 2, 3, 4}
}
