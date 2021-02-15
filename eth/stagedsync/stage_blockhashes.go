package stagedsync

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func extractHeaders(k []byte, v []byte, next etl.ExtractNextFunc) error {
	// We only want to extract entries composed by Block Number + Header Hash
	if len(k) != 40 {
		return nil
	}
	return next(k, common.CopyBytes(k[8:]), common.CopyBytes(k[:8]))
}

func SpawnBlockHashStage(s *StageState, db ethdb.Database, tmpdir string, quit <-chan struct{}) error {
	headNumber, err := stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}
	headHash := rawdb.ReadHeaderByNumber(db, headNumber).Hash()
	if s.BlockNumber == headNumber {
		s.Done()
		return nil
	}
	log.Info("BlockHashes", "from", s.BlockNumber, "headHash", headHash, "headNumber", headNumber)

	startKey := make([]byte, 8)
	binary.BigEndian.PutUint64(startKey, s.BlockNumber)
	endKey := dbutils.HeaderKey(headNumber, headHash) // Make sure we stop at head

	logPrefix := s.state.LogPrefix()
	if err := etl.Transform(
		logPrefix,
		db,
		dbutils.HeaderPrefix,
		dbutils.HeaderNumberPrefix,
		tmpdir,
		extractHeaders,
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			ExtractStartKey: startKey,
			ExtractEndKey:   endKey,
			Quit:            quit,
		},
	); err != nil {
		return err
	}
	return s.DoneAndUpdate(db, headNumber)
}
