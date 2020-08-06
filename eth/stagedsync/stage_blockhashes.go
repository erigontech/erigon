package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func extractHeaders(k []byte, v []byte, next etl.ExtractNextFunc) error {
	if len(k) != 40 {
		return nil
	}
	return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
}

func SpawnBlockHashStage(s *StageState, stateDB ethdb.Database, quit <-chan struct{}) error {
	firstHash := common.BytesToHash(s.StageData)
	number := rawdb.ReadHeaderNumber(stateDB, firstHash)
	var startKey []byte
	if number == nil {
		startKey = nil
	} else {
		startKey = dbutils.HeaderKey(*number, firstHash)
	}

	headHash := rawdb.ReadHeadHeaderHash(stateDB)
	headNumber := rawdb.ReadHeaderNumber(stateDB, headHash)
	endKey := dbutils.HeaderKey(*headNumber, headHash)

	onLoadCommit := func(db ethdb.Putter, key []byte, isDone bool) error {
		if len(key) == 0 {
			return nil
		}
		return s.UpdateWithStageData(db, 0, key)
	}
	if err := etl.Transform(
		stateDB,
		dbutils.HeaderPrefix,
		dbutils.HeaderNumberPrefix,
		".",
		extractHeaders,
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			ExtractStartKey: startKey,
			ExtractEndKey:   endKey,
			Quit:            quit,
			OnLoadCommit:    onLoadCommit,
		},
	); err != nil {
		return err
	}
	s.Done()
	return nil
}
