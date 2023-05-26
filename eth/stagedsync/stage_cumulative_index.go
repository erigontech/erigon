package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
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

func SpawnStageCumulativeIndex(cfg CumulativeIndexCfg, s *StageState, tx kv.RwTx, ctx context.Context, logger log.Logger) error {
	useExternalTx := tx != nil

	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	// Log timer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting bodies progress: %w", err)
	}
	// If we are done already, we can exit the stage
	if s.BlockNumber == headNumber {
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

	currentBlockNumber := s.BlockNumber + 1
	for k, v, err := headerC.Seek(hexutility.EncodeTs(s.BlockNumber)); k != nil; k, v, err = headerC.Next() {
		if err != nil {
			return err
		}

		if len(k) != 40 {
			continue
		}

		blockNumber, err := dbutils.DecodeBlockNumber(k[:8])
		if err != nil {
			return err
		}

		canonicalHash, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return err
		}

		if canonicalHash != libcommon.BytesToHash(k[8:]) {
			continue
		}

		currentBlockNumber = blockNumber

		var header types.Header
		if err := rlp.Decode(bytes.NewReader(v), &header); err != nil {
			return err
		}
		cumulativeGasUsed.Add(cumulativeGasUsed, big.NewInt(int64(header.GasUsed)))

		if err := rawdb.WriteCumulativeGasUsed(tx, currentBlockNumber, cumulativeGasUsed); err != nil {
			return err
		}

		// Check for logs
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[%s] Wrote Cumulative Index", s.LogPrefix()),
				"gasUsed", cumulativeGasUsed.String(), "now", currentBlockNumber, "blk/sec", float64(currentBlockNumber-prevProgress)/float64(logInterval/time.Second))
			prevProgress = currentBlockNumber
		default:
			logger.Trace("RequestQueueTime (header) ticked")
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

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
