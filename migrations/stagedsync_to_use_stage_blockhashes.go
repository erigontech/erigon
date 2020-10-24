package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var stagedsyncToUseStageBlockhashes = Migration{
	Name: "stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {

		var stageProgress uint64
		var err error
		if stageProgress, _, err = stages.GetStageProgress(db, stages.Headers); err != nil {
			return err
		}

		if err = stages.SaveStageProgress(db, stages.BlockHashes, stageProgress, nil); err != nil {
			return err
		}

		if err = OnLoadCommit(db, nil, true); err != nil {
			return err
		}

		return nil
	},
}

var unwindStagedsyncToUseStageBlockhashes = Migration{
	Name: "unwind_stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, OnLoadCommit etl.LoadCommitHandler) error {

		var stageProgress uint64
		var err error
		if stageProgress, _, err = stages.GetStageUnwind(db, stages.Headers); err != nil {
			return err
		}

		if err = stages.SaveStageUnwind(db, stages.BlockHashes, stageProgress, nil); err != nil {
			return err
		}

		if err = OnLoadCommit(db, nil, true); err != nil {
			return err
		}

		return nil
	},
}
