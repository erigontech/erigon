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
		if err := batch.Put(dbutils.HeaderNumberPrefix, k[8:], k[:8]); err != nil {
			return false, err
		}
		if batch.BatchSize() > batch.IdealBatchSize() || bytes.Equal(k[8:], headHash) {
			if err := s.UpdateWithStageData(batch, progress, k[8:]); err != nil {
				return false, err
			}
			log.Info("Committed block hashes", "number", progress)
			_, err := batch.Commit()
			if err != nil {
				return false, err
			}
			if bytes.Equal(k[8:], headHash) {
				return true, nil
			}
		}
		progress++
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
	log.Info("Committed block hashes", "number", progress)
	// Write here code
	return nil
}
