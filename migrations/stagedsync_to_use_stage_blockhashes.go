package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var stagedsyncToUseStageBlockhashes = Migration{
	Name: "stagedsync_to_use_stage_blockhashes",
	Up: func(db ethdb.Database, datadir string, OnLoadCommit etl.LoadCommitHandler) error {
		headHash := rawdb.ReadHeadHeaderHash(db)
		headNumber := rawdb.ReadHeaderNumber(db, headHash)
		if headNumber == nil {
			return nil
		}
		headHeader := rawdb.ReadHeader(db, headHash, *headNumber)
		if rawdb.ReadHeaderNumber(db, headHeader.ParentHash) == nil {
			return nil
		}

		return stages.SaveStageProgress(db, stages.BlockHashes, *headNumber, nil)
	},
}
