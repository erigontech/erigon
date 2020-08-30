package stagedsync

import (
	"time"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

const prof = false // whether to profile

type StagedSync struct {
}

func New() *StagedSync {
	return &StagedSync{}
}

func (*StagedSync) Prepare(
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

	stages := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Download headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnHeaderDownloadStage(s, u, d, headersFetchers)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(db)
			},
		},
		{
			ID:          stages.BlockHashes,
			Description: "Write block hashes",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnBlockHashStage(s, db, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(db)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Download block bodies",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return spawnBodyDownloadStage(s, u, d, pid)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindBodyDownloadStage(u, db)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recover senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				const batchSize = 10000
				const blockSize = 4096
				n := secp256k1.NumOfContexts() // we can only be as parallels as our crypto library supports

				cfg := Stage3Config{
					BatchSize:       batchSize,
					BlockSize:       blockSize,
					BufferSize:      (blockSize * 10 / 20) * 10000, // 20*4096
					StartTrace:      false,
					Prof:            false,
					NumOfGoroutines: n,
					ReadChLen:       4,
					Now:             time.Now(),
				}
				return SpawnRecoverSendersStage(cfg, s, tx, chainConfig, 0, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindSendersStage(u, tx)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Execute blocks w/o hash checks",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnExecuteBlocksStage(s, tx, chainConfig, chainContext, vmConfig, 0 /* limit (meaning no limit) */, quitCh, storageMode.Receipts, changeSetHook)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindExecutionStage(u, s, tx, storageMode.Receipts)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hash the key in the state",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnHashStateStage(s, tx, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindHashStateStage(u, s, tx, datadir, quitCh)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generate intermediate hashes and computing state root",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnIntermediateHashesStage(s, tx, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindIntermediateHashesStage(u, s, tx, datadir, quitCh)
			},
		},
		{
			ID:                  stages.AccountHistoryIndex,
			Description:         "Generate account history index",
			Disabled:            !storageMode.History,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnAccountHistoryIndex(s, tx, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindAccountHistoryIndex(u, tx, quitCh)
			},
		},
		{
			ID:                  stages.StorageHistoryIndex,
			Description:         "Generate storage history index",
			Disabled:            !storageMode.History,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnStorageHistoryIndex(s, tx, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindStorageHistoryIndex(u, tx, quitCh)
			},
		},
		{
			ID:                  stages.TxLookup,
			Description:         "Generate tx lookup index",
			Disabled:            !storageMode.TxIndex,
			DisabledDescription: "Enable by adding `t` to --storage-mode",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnTxLookup(s, tx, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return UnwindTxLookup(u, s, tx, datadir, quitCh)
			},
		},
		{
			ID:          stages.TxPool,
			Description: "Update transaction pool",
			ExecFunc: func(s *StageState, _ Unwinder) error {
				return spawnTxPool(s, tx, txPool, poolStart, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindTxPool(u, s, tx, txPool, quitCh)
			},
		},
		{
			ID:          stages.Finish,
			Description: "Final: update current block for the RPC API",
			ExecFunc: func(s *StageState, _ Unwinder) error {
				var executionAt uint64
				var err error
				if executionAt, err = s.ExecutionAt(tx); err != nil {
					return err
				}
				return s.DoneAndUpdate(tx, executionAt)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				var executionAt uint64
				var err error
				if executionAt, err = s.ExecutionAt(tx); err != nil {
					return err
				}
				return s.DoneAndUpdate(tx, executionAt)
			},
		},
	}

	state := NewState(stages)
	state.unwindOrder = []*Stage{
		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
		// also tx pool is before senders because senders unwind is inside cycle transaction
		// Unwinding of IHashes needs to happen after unwinding HashState
		stages[0], stages[1], stages[2], stages[10], stages[3], stages[4], stages[6], stages[5], stages[7], stages[8], stages[9],
	}
	if err := state.LoadUnwindInfo(db); err != nil {
		return nil, err
	}
	return state, nil
}
