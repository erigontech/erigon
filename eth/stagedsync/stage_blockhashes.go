package stagedsync

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func SpawnBlockHashStage(s *StageState, stateDB ethdb.Database, quit <-chan struct{}) error {
	progress := s.BlockNumber
	batch := stateDB.NewBatch()
	firstHash := s.StageData
	headHash := rawdb.ReadHeadHeaderHash(stateDB).Bytes()
	if err := stateDB.Walk(dbutils.HeaderPrefix, dbutils.HeaderKey(progress, common.BytesToHash(firstHash)), 0, func(k []byte, _ []byte) (bool, error) {
		if err := common.Stopped(quit); err != nil {
			return true, err
		}

		if len(k) != 40 {
			return true, nil
		}
		hash := common.CopyBytes(k[8:])
		number := common.CopyBytes(k[:8])
		if err := batch.Put(dbutils.HeaderNumberPrefix, hash, number); err != nil {
			return false, err
		}
		if batch.BatchSize() > batch.IdealBatchSize() || bytes.Equal(hash, headHash) {
			if err := s.UpdateWithStageData(batch, progress, hash); err != nil {
				return false, err
			}
			log.Info("Committed block hashes", "number", progress)
			_, err := batch.Commit()
			if err != nil {
				return false, err
			}
			if bytes.Equal(hash, headHash) {
				return true, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}

	if err := s.UpdateWithStageData(batch, progress, headHash); err != nil {
		return err
	}
	_, err := batch.Commit()
	if err != nil {
		return err
	}
	s.Done()
	// Write here code
	return nil
}
