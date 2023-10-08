package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ots/indexer"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

// Handles block indexer lifecycle.
type BlockIndexerHandler[T any] interface {
	// Given a tx that must be indexed, handles all logs that caused the matching.
	HandleMatch(body *types.Body)

	Flush(force bool) error

	Load(ctx context.Context, tx kv.RwTx) error

	// Disposes the allocated resources
	Close()
}

// TODO: extract common logic from runIncrementalLogIndexerExecutor
func runIncrementalBlockIndexerExecutor[T any](db kv.RoDB, tx kv.RwTx, blockReader services.FullBlockReader, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, handler BlockIndexerHandler[T]) (uint64, error) {
	// Tracks how many blocks finished analysis so far
	totalBlocks := uint64(0)

	// Tracks how many blocks finished analysis with a match so far
	totalMatch := uint64(0)

	// Process control
	flushEvery := time.NewTicker(bitmapsFlushEvery)
	defer flushEvery.Stop()

	///////////////
	// WITHDRAW EXCLUSIVE
	///////////////
	withdrawalIdx2Block, err := tx.RwCursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return startBlock, err
	}
	defer withdrawalIdx2Block.Close()

	withdrawals, err := tx.RwCursorDupSort(kv.OtsWithdrawalsIndex)
	if err != nil {
		return startBlock, err
	}
	defer withdrawals.Close()
	///////////////

	// Iterate over all blocks [startBlock, endBlock]
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		hash, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return startBlock, err
		}
		body, _, err := blockReader.Body(ctx, tx, hash, blockNum)
		if err != nil {
			return startBlock, err
		}

		withdrawals := body.Withdrawals
		totalBlocks++
		if len(withdrawals) == 0 {
			continue
		}
		totalMatch++
		last := withdrawals[len(withdrawals)-1]

		k := hexutility.EncodeTs(last.Index)
		v := hexutility.EncodeTs(blockNum)
		if err := withdrawalIdx2Block.Put(k, v); err != nil {
			return startBlock, err
		}
		handler.HandleMatch(body)

		select {
		default:
		case <-ctx.Done():
			return startBlock, common.ErrStopped
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Scanning blocks", s.LogPrefix()), "block", blockNum, "matches", totalMatch, "blocks", totalBlocks)
		case <-flushEvery.C:
			if err := handler.Flush(false); err != nil {
				return startBlock, err
			}
		}
	}

	// Last (forced) flush and batch load (if applicable)
	if err := handler.Flush(true); err != nil {
		return startBlock, err
	}
	if err := handler.Load(ctx, tx); err != nil {
		return startBlock, err
	}

	// Don't print summary if no contracts were analyzed to avoid polluting logs
	if !isShortInterval && totalBlocks > 0 {
		log.Info(fmt.Sprintf("[%s] Totals", s.LogPrefix()), "matches", totalMatch, "blocks", totalBlocks)
	}

	return endBlock, nil
}

// Implements BlockIndexerHandler interface in order to index block withdrawals from CL
type WithdrawalsIndexerHandler struct {
	indexBucket          string
	counterBucket        string
	withdrawalsCollector *etl.Collector
	withdrawalsBitmap    map[string]*roaring64.Bitmap
}

func NewWithdrawalsIndexerHandler(tmpDir string, s *StageState, logger log.Logger) BlockIndexerHandler[TransferAnalysisResult] {
	withdrawalsCollector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	withdrawalsBitmap := map[string]*roaring64.Bitmap{}

	return &WithdrawalsIndexerHandler{kv.OtsWithdrawalsIndex, kv.OtsWithdrawalsCounter, withdrawalsCollector, withdrawalsBitmap}
}

// Add log's ethTx index to from/to addresses indexes
func (h *WithdrawalsIndexerHandler) HandleMatch(body *types.Body) {
	for _, w := range body.Withdrawals {
		h.touchIndex(w)
	}
}

func (h *WithdrawalsIndexerHandler) touchIndex(withdrawal *types.Withdrawal) {
	addr := withdrawal.Address
	bm, ok := h.withdrawalsBitmap[string(addr.Bytes())]
	if !ok {
		bm = roaring64.NewBitmap()
		h.withdrawalsBitmap[string(addr.Bytes())] = bm
	}
	bm.Add(withdrawal.Index)
}

func (h *WithdrawalsIndexerHandler) Flush(force bool) error {
	if force || needFlush64(h.withdrawalsBitmap, bitmapsBufLimit) {
		if err := flushBitmaps64(h.withdrawalsCollector, h.withdrawalsBitmap); err != nil {
			return err
		}
		h.withdrawalsBitmap = map[string]*roaring64.Bitmap{}
	}

	return nil
}

func (h *WithdrawalsIndexerHandler) Load(ctx context.Context, tx kv.RwTx) error {
	transferCounter, err := tx.RwCursorDupSort(h.counterBucket)
	if err != nil {
		return err
	}
	defer transferCounter.Close()

	buf := bytes.NewBuffer(nil)
	addrBm := roaring64.NewBitmap()

	loadFunc := func(k []byte, value []byte, tableReader etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Bitmap for address key
		if _, err := addrBm.ReadFrom(bytes.NewBuffer(value)); err != nil {
			return err
		}

		// Last chunk for address key
		addr := k[:length.Addr]

		// Read last chunk from DB (may not exist)
		// Chunk already exists; merge it
		if err := mergeLastChunk(addrBm, addr, tableReader); err != nil {
			return err
		}

		// Recover and delete the last counter (may not exist); will be replaced after this chunk write
		prevCounter := uint64(0)
		isUniqueChunk := false
		counterK, _, err := transferCounter.SeekExact(addr)
		if err != nil {
			return err
		}
		if counterK != nil {
			counterV, err := transferCounter.LastDup()
			if err != nil {
				return err
			}
			if len(counterV) == 1 {
				// Optimized counter; prevCounter must remain 0
				c, err := transferCounter.CountDuplicates()
				if err != nil {
					return err
				}
				if c != 1 {
					return fmt.Errorf("db possibly corrupted: bucket=%s addr=%s has optimized counter with duplicates", h.counterBucket, hexutility.Encode(addr))
				}

				isUniqueChunk = true
			} else {
				// Regular counter
				chunk := counterV[8:]
				chunkAsNumber := binary.BigEndian.Uint64(chunk)
				if chunkAsNumber != ^uint64(0) {
					return fmt.Errorf("db possibly corrupted: bucket=%s addr=%s last chunk is not 0xffffffffffffffff: %s", h.counterBucket, hexutility.Encode(addr), hexutility.Encode(chunk))
				}
			}

			// Delete last counter, optimized or not; it doesn't matter, it'll be
			// rewriten below
			if err := transferCounter.DeleteCurrent(); err != nil {
				return err
			}

			// Regular chunk, rewind to previous counter
			if !isUniqueChunk {
				prevK, prevV, err := transferCounter.PrevDup()
				if err != nil {
					return err
				}
				if prevK != nil {
					prevCounter = binary.BigEndian.Uint64(prevV[:8])
				}
			}
		}

		// Write the index chunk; cut it if necessary to fit under page restrictions
		if (counterK == nil || isUniqueChunk) && prevCounter+addrBm.GetCardinality() <= 256 {
			buf.Reset()
			b := make([]byte, 8)
			for it := addrBm.Iterator(); it.HasNext(); {
				ethTx := it.Next()
				binary.BigEndian.PutUint64(b, ethTx)
				buf.Write(b)
			}

			_, err := h.writeOptimizedChunkAndCounter(tx, k, buf, addr, next, prevCounter)
			if err != nil {
				return err
			}
		} else {
			buf.Reset()
			b := make([]byte, 8)
			for it := addrBm.Iterator(); it.HasNext(); {
				ethTx := it.Next()
				binary.BigEndian.PutUint64(b, ethTx)
				buf.Write(b)

				// cut?
				if !it.HasNext() || buf.Len() >= int(bitmapdb.ChunkLimit) {
					updatedCounter, err := h.writeRegularChunkAndCounter(tx, k, buf, addr, next, ethTx, !it.HasNext(), prevCounter)
					if err != nil {
						return err
					}
					prevCounter = updatedCounter

					// Cleanup buffer for next chunk
					buf.Reset()
				}
			}
		}

		return nil
	}
	if err := h.withdrawalsCollector.Load(tx, h.indexBucket, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return nil
}

func (h *WithdrawalsIndexerHandler) writeOptimizedChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, prevCounter uint64) (uint64, error) {
	// Write solo chunk
	chunkKey := chunkKey(k, true, 0)
	if err := next(k, chunkKey, buf.Bytes()); err != nil {
		return 0, err
	}

	// Write optimized counter
	prevCounter += uint64(buf.Len()) / 8
	v := indexer.OptimizedCounterSerializer(prevCounter)
	if err := tx.Put(h.counterBucket, addr, v); err != nil {
		return 0, err
	}

	return prevCounter, nil
}

func (h *WithdrawalsIndexerHandler) writeRegularChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, ethTx uint64, isLast bool, prevCounter uint64) (uint64, error) {
	chunkKey := chunkKey(k, isLast, ethTx)
	if err := next(k, chunkKey, buf.Bytes()); err != nil {
		return 0, err
	}

	// Write updated counter
	prevCounter += uint64(buf.Len()) / 8
	v := indexer.RegularCounterSerializer(prevCounter, chunkKey[length.Addr:])
	if err := tx.Put(h.counterBucket, addr, v); err != nil {
		return 0, err
	}

	return prevCounter, nil
}

func (h *WithdrawalsIndexerHandler) Close() {
	h.withdrawalsCollector.Close()
}
