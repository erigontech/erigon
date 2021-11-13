package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
)

func SpawnDifficultyStage(s *StageState, tx kv.RwTx, tmpDir string, ctx context.Context) (err error) {
	useExternalTx := tx != nil

	quit := ctx.Done()
	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}

	td := common.Big0
	if s.BlockNumber > 0 {
		td, err = rawdb.ReadTd(tx, rawdb.ReadHeaderByNumber(tx, s.BlockNumber).Hash(), s.BlockNumber)
		if err != nil {
			return err
		}
	}

	startKey := make([]byte, 8)
	binary.BigEndian.PutUint64(startKey, s.BlockNumber)

	logPrefix := s.LogPrefix()
	// Canonical hashes (for checking)
	canonicalC, err := tx.Cursor(kv.HeaderCanonical)
	if err != nil {
		return err
	}
	canonical := make([]common.Hash, headNumber-s.BlockNumber)
	currentHeaderIdx := uint64(0)

	for k, v, err := canonicalC.Seek(dbutils.EncodeBlockNumber(s.BlockNumber + 1)); k != nil; k, v, err = canonicalC.Next() {
		if err != nil {
			return err
		}
		if err := libcommon.Stopped(quit); err != nil {
			return err
		}

		if currentHeaderIdx >= headNumber-s.BlockNumber { // if header stage is ehead of body stage
			break
		}

		copy(canonical[currentHeaderIdx][:], v)
		currentHeaderIdx++
	}

	currentHeaderIdx = 0
	if err := etl.Transform(
		logPrefix,
		tx,
		kv.Headers,
		kv.HeaderTD,
		tmpDir,
		func(k []byte, v []byte, next etl.ExtractNextFunc) error {
			if len(k) != 40 {
				return nil
			}

			// If not canonical, then we skip it
			if !bytes.Equal(k[8:], canonical[currentHeaderIdx][:]) {
				fmt.Println(currentHeaderIdx)
				return nil
			}
			currentHeaderIdx++
			header := new(types.Header)

			if err := rlp.Decode(bytes.NewReader(v), header); err != nil {
				return err
			}
			td.Add(td, header.Difficulty)
			data, err := rlp.EncodeToBytes(td)
			if err != nil {
				return fmt.Errorf("failed to RLP encode block total difficulty: %w", err)
			}
			return next(k, k, data)
		},
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			ExtractStartKey: startKey,
			Quit:            quit,
		},
	); err != nil {
		return err
	}
	if err = s.Update(tx, headNumber); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindDifficultyStage(u *UnwindState, tx kv.RwTx, cfg BlockHashesCfg, ctx context.Context) (err error) {
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

func PruneDifficultyStage(p *PruneState, tx kv.RwTx, cfg BlockHashesCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
