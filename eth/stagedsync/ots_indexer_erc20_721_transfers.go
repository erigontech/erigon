package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/ots/indexer"
	"github.com/ledgerwatch/log/v3"
)

// Handles ERC20 and ERC721 indexing simultaneously
type MultiLogIndexerHandler[T any] struct {
	handlers []LogIndexerHandler[T]
}

func NewMultiIndexerHandler[T any](handlers ...LogIndexerHandler[T]) *MultiLogIndexerHandler[T] {
	return &MultiLogIndexerHandler[T]{
		handlers,
	}
}

func (c *MultiLogIndexerHandler[T]) HandleMatch(output *TxMatchedLogs[T]) {
	for _, h := range c.handlers {
		h.HandleMatch(output)
	}
}

func (c *MultiLogIndexerHandler[T]) Flush(force bool) error {
	for _, h := range c.handlers {
		if err := h.Flush(force); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiLogIndexerHandler[T]) Load(ctx context.Context, tx kv.RwTx) error {
	for _, h := range c.handlers {
		if err := h.Load(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiLogIndexerHandler[T]) Close() {
	for _, h := range c.handlers {
		h.Close()
	}
}

// Implements LogIndexerHandler interface in order to index token transfers
// (ERC20/ERC721)
type TransferLogIndexerHandler struct {
	nft                bool
	indexBucket        string
	counterBucket      string
	transfersCollector *etl.Collector
	transfersBitmap    map[string]*roaring64.Bitmap
}

func NewTransferLogIndexerHandler(tmpDir string, s *StageState, nft bool, indexBucket, counterBucket string, logger log.Logger) LogIndexerHandler[TransferAnalysisResult] {
	transfersCollector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	transfersBitmap := map[string]*roaring64.Bitmap{}

	return &TransferLogIndexerHandler{nft, indexBucket, counterBucket, transfersCollector, transfersBitmap}
}

// Add log's ethTx index to from/to addresses indexes
func (h *TransferLogIndexerHandler) HandleMatch(match *TxMatchedLogs[TransferAnalysisResult]) {
	for _, res := range match.matchResults {
		if res.nft != h.nft {
			continue
		}

		// Register this ethTx into from/to transfer addresses indexes
		h.touchIndex(res.from, match.ethTx)
		h.touchIndex(res.to, match.ethTx)
	}
}

func (h *TransferLogIndexerHandler) touchIndex(addr common.Address, ethTx uint64) {
	bm, ok := h.transfersBitmap[string(addr.Bytes())]
	if !ok {
		bm = roaring64.NewBitmap()
		h.transfersBitmap[string(addr.Bytes())] = bm
	}
	bm.Add(ethTx)
}

func (h *TransferLogIndexerHandler) Flush(force bool) error {
	if force || needFlush64(h.transfersBitmap, bitmapsBufLimit) {
		if err := flushBitmaps64(h.transfersCollector, h.transfersBitmap); err != nil {
			return err
		}
		h.transfersBitmap = map[string]*roaring64.Bitmap{}
	}

	return nil
}

func (h *TransferLogIndexerHandler) Load(ctx context.Context, tx kv.RwTx) error {
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
	if err := h.transfersCollector.Load(tx, h.indexBucket, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return nil
}

// Reads the last index chunk for a certain address and merge the result
// into the currently being processed bitmap.
func mergeLastChunk(addrBm *roaring64.Bitmap, addr []byte, tableReader etl.CurrentTableReader) error {
	chunkBm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(chunkBm)

	key := make([]byte, length.Addr+8)
	copy(key, addr)
	binary.BigEndian.PutUint64(key[length.Addr:], ^uint64(0))

	// Read last chunk from DB (may not exist)
	v, err := tableReader.Get(key)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	for i := 0; i < len(v); i += 8 {
		chunkBm.Add(binary.BigEndian.Uint64(v[i : i+8]))
	}
	addrBm.Or(chunkBm)

	return nil
}

// k == address [length.Addr]byte + chunk uint64
func chunkKey(k []byte, isLast bool, ethTx uint64) []byte {
	key := make([]byte, length.Addr+8)
	copy(key, k[:length.Addr])

	if isLast {
		binary.BigEndian.PutUint64(key[length.Addr:], ^uint64(0))
	} else {
		binary.BigEndian.PutUint64(key[length.Addr:], ethTx)
	}

	return key
}

func (h *TransferLogIndexerHandler) writeOptimizedChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, prevCounter uint64) (uint64, error) {
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

func (h *TransferLogIndexerHandler) writeRegularChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, ethTx uint64, isLast bool, prevCounter uint64) (uint64, error) {
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

func (h *TransferLogIndexerHandler) Close() {
	h.transfersCollector.Close()
}
