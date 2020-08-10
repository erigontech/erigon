package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var stagedsyncToUseStageBlockhashes = Migration{
	Name: "stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {

		var progress uint64
		var err error
		if progress, _, err = stages.GetStageProgress(db, stages.Headers); err != nil {
			return err
		}

		if err = stages.SaveStageProgress(db, stages.BlockHashes, progress, nil); err != nil {
			return err
		}

		return nil
	},
}

var unwindStagedsyncToUseStageBlockhashes = Migration{
	Name: "unwind_stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {

		var progress uint64
		var err error
		if progress, _, err = stages.GetStageUnwind(db, stages.Headers); err != nil {
			return err
		}

		if err = stages.SaveStageUnwind(db, stages.BlockHashes, progress, nil); err != nil {
			return err
		}

		return nil
	},
}
