// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

type WitnessData struct {
	BlockNumber uint64
	BlockHash   common.Hash
	Data        []byte
}

type WitnessBuffer struct {
	buffer []WitnessData
	mutex  sync.RWMutex
}

func NewWitnessBuffer() *WitnessBuffer {
	return &WitnessBuffer{
		buffer: make([]WitnessData, 0),
	}
}

func (wb *WitnessBuffer) AddWitness(blockNumber uint64, blockHash common.Hash, data []byte) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	wb.buffer = append(wb.buffer, WitnessData{
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		Data:        data,
	})
}

func (wb *WitnessBuffer) DrainWitnesses() []WitnessData {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	witnesses := make([]WitnessData, len(wb.buffer))
	copy(witnesses, wb.buffer)

	wb.buffer = wb.buffer[:0]

	return witnesses
}

type WitnessProcessingCfg struct {
	db            kv.RwDB
	witnessBuffer *WitnessBuffer
}

func NewWitnessProcessingCfg(db kv.RwDB, witnessBuffer *WitnessBuffer) WitnessProcessingCfg {
	return WitnessProcessingCfg{
		db:            db,
		witnessBuffer: witnessBuffer,
	}
}

func StageWitnessProcessingCfg(db kv.RwDB, chainConfig *chain.Config, witnessBuffer *WitnessBuffer) *WitnessProcessingCfg {
	if chainConfig.Bor != nil && witnessBuffer != nil {
		cfg := NewWitnessProcessingCfg(db, witnessBuffer)
		return &cfg
	}

	return nil
}

// SpawnStageWitnessProcessing processes buffered witness data and stores it in the database
func SpawnStageWitnessProcessing(s *StageState, tx kv.RwTx, cfg WitnessProcessingCfg, ctx context.Context, logger log.Logger) error {
	if cfg.witnessBuffer == nil {
		return nil
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	// Drain all buffered witnesses
	witnesses := cfg.witnessBuffer.DrainWitnesses()

	if len(witnesses) == 0 {
		// No witnesses to process
		return nil
	}

	logger.Debug("[WitnessProcessing] processing witnesses", "count", len(witnesses))

	for _, witness := range witnesses {
		key := dbutils.HeaderKey(witness.BlockNumber, witness.BlockHash)

		if err := tx.Put(kv.BorWitnesses, key, witness.Data); err != nil {
			return err
		}

		sizeBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(sizeBytes, uint64(len(witness.Data)))
		if err := tx.Put(kv.BorWitnessSizes, key, sizeBytes); err != nil {
			return err
		}
	}

	if len(witnesses) > 0 {
		highestBlock := witnesses[0].BlockNumber
		for _, witness := range witnesses {
			if witness.BlockNumber > highestBlock {
				highestBlock = witness.BlockNumber
			}
		}

		if err := stages.SaveStageProgress(tx, stages.WitnessProcessing, highestBlock); err != nil {
			return err
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	logger.Info("[WitnessProcessing] completed witness processing", "processed", len(witnesses))
	return nil
}

// UnwindWitnessProcessingStage handles unwind operations for witness processing
func UnwindWitnessProcessingStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg WitnessProcessingCfg, logger log.Logger) error {
	var tx kv.RwTx
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	} else {
		tx = txc.Tx
	}

	if err := cleanupWitnessesForUnwind(tx, u.UnwindPoint+1); err != nil {
		logger.Warn("failed to cleanup witnesses during witness stage unwind", "err", err, "unwind_point", u.UnwindPoint)
		return err
	}

	if err := u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// PruneWitnessProcessingStage handles pruning for witness processing
func PruneWitnessProcessingStage(p *PruneState, tx kv.RwTx, cfg WitnessProcessingCfg, ctx context.Context, logger log.Logger) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	// Prune old witness data based on retention policy
	if err := cleanupOldWitnesses(tx, p.ForwardProgress, logger); err != nil {
		return err
	}

	if err := p.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// cleanupOldWitnesses removes witness data older than the retention period
func cleanupOldWitnesses(tx kv.RwTx, currentBlockNum uint64, logger log.Logger) error {
	if currentBlockNum <= wit.RetentionBlocks {
		return nil
	}

	cutoffBlockNum := currentBlockNum - wit.RetentionBlocks
	logger.Debug("cleaning up old witness data", "current_block", currentBlockNum, "cutoff_block", cutoffBlockNum)

	cursor, err := tx.RwCursor(kv.BorWitnesses)
	if err != nil {
		return fmt.Errorf("failed to create BorWitnesses cursor: %w", err)
	}
	defer cursor.Close()

	deletedCount := 0
	for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
		if err != nil {
			return fmt.Errorf("error iterating BorWitnesses: %w", err)
		}

		blockNum := binary.BigEndian.Uint64(k[:8])
		if blockNum < cutoffBlockNum {
			if err := cursor.DeleteCurrent(); err != nil {
				return fmt.Errorf("failed to delete witness: %w", err)
			}
			if err := tx.Delete(kv.BorWitnessSizes, k); err != nil {
				return fmt.Errorf("failed to delete witness size: %w", err)
			}
			deletedCount++
		} else {
			break
		}
	}

	if deletedCount > 0 {
		logger.Debug("cleaned up old witness data", "deleted_count", deletedCount, "cutoff_block", cutoffBlockNum)
	}

	return nil
}

// cleanupWitnessesForUnwind removes witness data for blocks that are being unwound
func cleanupWitnessesForUnwind(tx kv.RwTx, fromBlock uint64) error {
	cursor, err := tx.RwCursor(kv.BorWitnesses)
	if err != nil {
		return fmt.Errorf("failed to create BorWitnesses cursor: %w", err)
	}
	defer cursor.Close()

	deletedCount := 0
	for k, _, err := cursor.Seek(hexutil.EncodeTs(fromBlock)); k != nil; k, _, err = cursor.Next() {
		if err != nil {
			return fmt.Errorf("error iterating BorWitnesses: %w", err)
		}
		if err := cursor.DeleteCurrent(); err != nil {
			return fmt.Errorf("failed to delete witness during unwind: %w", err)
		}
		if err := tx.Delete(kv.BorWitnessSizes, k); err != nil {
			return fmt.Errorf("failed to delete witness size during unwind: %w", err)
		}
		deletedCount++
	}

	if deletedCount > 0 {
		log.Debug("cleaned up witnesses during unwind", "deleted_count", deletedCount, "from_block", fromBlock)
	}

	return nil
}
