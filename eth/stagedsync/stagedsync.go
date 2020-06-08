package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func PrepareStagedSync(
	d DownloaderGlue,
	blockchain BlockChain,
	stateDB ethdb.Database,
	pid string,
	history bool,
	datadir string,
	quitCh chan struct{},
	headersFetchers []func() error,
	dests vm.Cache,
) (*State, error) {
	defer log.Info("Staged sync finished")

	stages := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnHeaderDownloadStage(s, u, d, headersFetchers)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return spawnBodyDownloadStage(s, u, d, pid)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindBodyDownloadStage(stateDB, u)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return spawnRecoverSendersStage(s, stateDB, blockchain.Config(), quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindSendersStage(stateDB, u.UnwindPoint)
			},
		},
		{
			ID:          stages.Execution,
			Description: "Executing blocks w/o hash checks",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnExecuteBlocksStage(s, stateDB, blockchain, 0 /* limit (meaning no limit) */, quitCh, dests)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindExecutionStage(u.UnwindPoint, stateDB)
			},
		},
		{
			ID:          stages.HashState,
			Description: "Hashing the key in the state",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnHashStateStage(s, stateDB, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindHashStateStage(u, s, stateDB, datadir, quitCh)
			},
		},
		{
			ID:          stages.IntermediateHashes,
			Description: "Generating intermediate hashes and validating final hash",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return SpawnIntermediateHashesStage(s, stateDB, datadir, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindIntermediateHashesStage(u, s, stateDB, datadir, quitCh)
			},
		},
		{
			ID:                  stages.AccountHistoryIndex,
			Description:         "Generating account history index",
			Disabled:            !history,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return spawnAccountHistoryIndex(s, stateDB, datadir, core.UsePlainStateExecution, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindAccountHistoryIndex(u.UnwindPoint, stateDB, core.UsePlainStateExecution, quitCh)
			},
		},
		{
			ID:                  stages.StorageHistoryIndex,
			Description:         "Generating storage history index",
			Disabled:            !history,
			DisabledDescription: "Enable by adding `h` to --storage-mode",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return spawnStorageHistoryIndex(s, stateDB, datadir, core.UsePlainStateExecution, quitCh)
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return unwindStorageHistoryIndex(u.UnwindPoint, stateDB, core.UsePlainStateExecution, quitCh)
			},
		},
	}

	state := NewState(stages)

	if err := state.LoadUnwindInfo(stateDB); err != nil {
		return nil, err
	}
	return state, nil
}
