package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"time"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
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
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Started", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished", logPrefix))

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

	// we can do this till the last block executed, since we get gas used from there
	executeProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return fmt.Errorf("getting progress: %w", err)
	}
	// If we are done already, we can exit the stage
	if s.BlockNumber == executeProgress {
		log.Info(fmt.Sprintf("[%s] Nothing new to process", logPrefix))
		return nil
	}

	cumulativeGasUsed, err := rawdb.ReadCumulativeGasUsed(tx, s.BlockNumber)
	if err != nil {
		return err
	}

	prevProgress := s.BlockNumber
	headerC, err := tx.Cursor(kv.Headers)
	if err != nil {
		return err
	}

	currentBlockNumber := s.BlockNumber
	for k, v, err := headerC.Seek(hexutility.EncodeTs(s.BlockNumber + 1)); k != nil; k, v, err = headerC.Next() {
		if err != nil {
			return err
		}

		if len(k) != 40 {
			continue
		}

		blockHash := libcommon.BytesToHash(k[8:])
		blockNumber, err := dbutils.DecodeBlockNumber(k[:8])
		if err != nil {
			return err
		}

		canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return err
		}

		if canonicalHash != blockHash {
			continue
		}

		currentBlockNumber = blockNumber

		var header types.Header
		if err := rlp.Decode(bytes.NewReader(v), &header); err != nil {
			return err
		}

		blockGasUsedBig := new(big.Int).SetUint64(header.GasUsed)
		cumulativeGasUsed.Add(cumulativeGasUsed, blockGasUsedBig)

		if err := rawdb.WriteCumulativeGasUsed(tx, currentBlockNumber, cumulativeGasUsed); err != nil {
			return err
		}

		// Check for logs
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Wrote Cumulative Index", s.LogPrefix()),
				"gasUsed", cumulativeGasUsed.String(), "now", currentBlockNumber, "blk/sec", float64(currentBlockNumber-prevProgress)/float64(logInterval/time.Second))
			prevProgress = currentBlockNumber
		default:
			log.Trace("RequestQueueTime (header) ticked")
		}
		// Cleanup timer
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

func UnwindCumulativeIndexStage(u *UnwindState, cfg CumulativeIndexCfg, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	log.Debug("Unwinding Cumulative Index")

	if err := rawdb.DeleteCumulativeGasUsed(tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("failed to delete cumulative gas used: %w", err)
	}

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

func PruneCumulativeIndexStage(p *PruneState, tx kv.RwTx, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	if err := rawdb.DeleteCumulativeGasUsed(tx, 0); err != nil {
		return fmt.Errorf("failed to delete cumulative gas used: %w", err)
	}

	if err := p.Done(tx); err != nil {
		return fmt.Errorf(" reset: %w", err)
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
