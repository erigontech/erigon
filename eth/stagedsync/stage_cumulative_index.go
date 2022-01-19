package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

type CumulativeIndexCfg struct {
	db kv.RwDB
}

func StageCumulativeIndexCfg(db kv.RwDB) CumulativeIndexCfg {
	return CumulativeIndexCfg{
		db: db,
	}
}

func SpawnStageCumulativeIndex(cfg CumulativeIndexCfg, s *StageState, tx kv.RwTx, ctx context.Context) error {
	useExternalTx := tx != nil

	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	// Log timer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	headNumber, err := stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return fmt.Errorf("getting bodies progress: %w", err)
	}
	// If we are done already, we can exit the stage
	if s.BlockNumber == headNumber {
		return nil
	}

	stopped := false
	prevProgress := s.BlockNumber
	currentBlockNumber := s.BlockNumber + 1
	for ; currentBlockNumber < headNumber && !stopped; currentBlockNumber++ {
		// read body + transactions
		hash, err := rawdb.ReadCanonicalHash(tx, currentBlockNumber)
		if err != nil {
			return err
		}
		body, _, txCount := rawdb.ReadBody(tx, hash, currentBlockNumber)
		if body == nil {
			return fmt.Errorf("could not find block body for number: %d", currentBlockNumber)
		}
		header := rawdb.ReadHeader(tx, hash, currentBlockNumber)

		if header == nil {
			return fmt.Errorf("could not find block header for number: %d", currentBlockNumber)
		}

		// Sleep and check for logs
		timer := time.NewTimer(1 * time.Nanosecond)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Wrote Cumulative Index", s.LogPrefix()),
				"now", currentBlockNumber, "blk/sec", float64(currentBlockNumber-prevProgress)/float64(logInterval/time.Second))
			prevProgress = currentBlockNumber
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		}
		// Cleanup timer
		timer.Stop()
	}
	if err = s.Update(tx, currentBlockNumber); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindIssuanceStage(u *UnwindState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if err = u.Done(tx); err != nil {
		return fmt.Errorf(" reset: %w", err)
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to write db commit: %w", err)
		}
	}
	return nil
}

func PruneIssuanceStage(p *PruneState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
