package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(s *StageState, ctx context.Context, db ethdb.Database, hd *headerdownload.HeaderDownload) error {
	files, buffer := hd.PrepareStageData()
	if len(files) == 0 && (buffer == nil || buffer.IsEmpty()) {
		return nil
	}

	logPrefix := s.LogPrefix()

	count := 0
	var highest uint64
	var headerProgress uint64
	var err error
	log.Info(fmt.Sprintf("[%s] Processing headers...", logPrefix), "from", headerProgress)
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	batch := tx.NewBatch()
	defer batch.Rollback()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var logBlock uint64

	headHash := rawdb.ReadHeadHeaderHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	localTd, err1 := rawdb.ReadTd(tx, headHash, *headNumber)
	if err1 != nil {
		return err1
	}
	var parentDiffs = make(map[common.Hash]*big.Int)
	var childDiffs = make(map[common.Hash]*big.Int)
	var prevHash common.Hash // Hash of previously seen header - to filter out potential duplicates
	var prevHeight uint64
	var newCanonical bool
	var forkNumber uint64
	var canonicalBacktrack = make(map[common.Hash]common.Hash)
	if err1 = headerdownload.ReadFilesAndBuffer(files, buffer, func(header *types.Header, blockHeight uint64) error {
		hash := header.Hash()
		if hash == prevHash {
			// Skip duplicates
			return nil
		}
		if ch, err := rawdb.ReadCanonicalHash(tx, blockHeight); err == nil {
			if ch == hash {
				// Already canonical, skip
				return nil
			}
		} else {
			return err
		}
		if blockHeight < prevHeight {
			return fmt.Errorf("[%s] headers are unexpectedly unsorted, got %d after %d", logPrefix, blockHeight, prevHeight)
		}
		if forkNumber == 0 {
			forkNumber = blockHeight
			logBlock = blockHeight - 1
		}
		if blockHeight > prevHeight {
			// Clear out parent map and move childMap to its place
			if blockHeight == prevHeight+1 {
				parentDiffs = childDiffs
			}
			childDiffs = make(map[common.Hash]*big.Int)
			prevHeight = blockHeight
		}
		parentDiff, ok := parentDiffs[header.ParentHash]
		if !ok {
			var err error
			if parentDiff, err = rawdb.ReadTd(tx, header.ParentHash, blockHeight-1); err != nil {
				return fmt.Errorf("[%s] reading total difficulty of the parent header %d %x: %w", logPrefix, blockHeight-1, header.ParentHash, err)
			}
			if parentDiff == nil {
				return fmt.Errorf("Could not find parentDiff for %d\n", header.Number)
			}
		}
		cumulativeDiff := new(big.Int).Add(parentDiff, header.Difficulty)
		childDiffs[hash] = cumulativeDiff
		if !newCanonical && cumulativeDiff.Cmp(localTd) > 0 {
			newCanonical = true
			backHash := header.ParentHash
			backNumber := blockHeight - 1
			for pHash, pOk := canonicalBacktrack[backHash]; pOk; pHash, pOk = canonicalBacktrack[backHash] {
				if err := rawdb.WriteCanonicalHash(batch, backHash, backNumber); err != nil {
					return fmt.Errorf("[%s] marking canonical header %d %x: %w", logPrefix, backNumber, backHash, err)
				}
				backHash = pHash
				backNumber--
			}
			canonicalBacktrack = nil
		}
		if !newCanonical {
			canonicalBacktrack[hash] = header.ParentHash
		}
		if newCanonical {
			if err := rawdb.WriteCanonicalHash(batch, hash, blockHeight); err != nil {
				return fmt.Errorf("[%s] marking canonical header %d %x: %w", logPrefix, blockHeight, hash, err)
			}
			if blockHeight > headerProgress {
				headerProgress = blockHeight
				if err := stages.SaveStageProgress(batch, stages.Headers, blockHeight); err != nil {
					return fmt.Errorf("[%s] saving Headers progress: %w", logPrefix, err)
				}
			}
			//rawdb.WriteHeadHeaderHash(batch, hash)
		}
		data, err := rlp.EncodeToBytes(header)
		if err != nil {
			return fmt.Errorf("[%s] Failed to RLP encode header: %w", logPrefix, err)
		}
		if err = rawdb.WriteTd(batch, hash, blockHeight, cumulativeDiff); err != nil {
			return fmt.Errorf("[%s] Failed to WriteTd: %w", logPrefix, err)
		}
		if err = batch.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(blockHeight, hash), data); err != nil {
			return fmt.Errorf("[%s] Failed to store header: %w", logPrefix, err)
		}
		prevHash = hash
		count++
		if blockHeight > highest {
			highest = blockHeight
		}
		if batch.BatchSize() >= batch.IdealBatchSize() {
			if err = batch.CommitAndBegin(context.Background()); err != nil {
				return err
			}
			if !useExternalTx {
				if err = tx.CommitAndBegin(context.Background()); err != nil {
					return err
				}
			}
		}
		select {
		default:
		case <-logEvery.C:
			logBlock = logProgressHeaders(logPrefix, logBlock, blockHeight, batch)
		}
		return nil
	}); err1 != nil {
		return err1
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if err := s.DoneAndUpdate(tx, highest); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info("Processed", "headers", count, "highest", highest)
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Error("Could not remove", "file", file, "error", err)
		}
	}
	return nil
}

func logProgressHeaders(logPrefix string, prev, now uint64, batch ethdb.DbWithPendingMutations) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block headers", logPrefix),
		"number", now,
		"blk/second", speed,
		"batch", common.StorageSize(batch.BatchSize()),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))

	return now
}
