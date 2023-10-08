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
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/ots/indexer"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

func NewGenericIndexerUnwinder(targetBucket, counterBucket string, attrs *roaring64.Bitmap) UnwindExecutor {
	return func(ctx context.Context, tx kv.RwTx, u *UnwindState, _ services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error {
		return runUnwind(ctx, tx, isShortInterval, logEvery, u, targetBucket, counterBucket, attrs)
	}
}

func runUnwind(ctx context.Context, tx kv.RwTx, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, targetBucket, counterBucket string, attrs *roaring64.Bitmap) error {
	target, err := tx.RwCursorDupSort(targetBucket)
	if err != nil {
		return err
	}
	defer target.Close()

	counter, err := tx.RwCursor(counterBucket)
	if err != nil {
		return err
	}
	defer counter.Close()

	// The unwind interval is ]u.UnwindPoint, EOF]
	startBlock := hexutility.EncodeTs(u.UnwindPoint + 1)

	// Delete all specified address attributes for affected addresses down to
	// unwind point + 1
	if attrs != nil {
		k, v, err := target.Seek(startBlock)
		if err != nil {
			return err
		}
		for k != nil {
			addr := common.BytesToAddress(v)
			if err := RemoveAttributes(tx, addr, attrs); err != nil {
				return err
			}

			k, v, err = target.NextDup()
			if err != nil {
				return err
			}
			if k == nil {
				k, v, err = target.NextNoDup()
				if err != nil {
					return err
				}
			}
		}
	}

	// Delete all block indexes backwards down to unwind point + 1
	unwoundBlock, err := unwindUint64KeyBasedDupTable(target, u.UnwindPoint)
	if err != nil {
		return err
	}

	// Delete all indexer counters backwards down to unwind point + 1
	k, v, err := counter.Last()
	if err != nil {
		return err
	}
	for k != nil {
		blockNum := binary.BigEndian.Uint64(v)
		if blockNum <= u.UnwindPoint {
			if blockNum != unwoundBlock {
				log.Error(fmt.Sprintf("[%s] Counter index is corrupt; please report as a bug", u.LogPrefix()), "unwindPoint", u.UnwindPoint, "blockNum", blockNum)
				return fmt.Errorf("[%s] Counter index is corrupt; please report as a bug: unwindPoint=%v blockNum=%v", u.LogPrefix(), u.UnwindPoint, blockNum)
			}
			break
		}

		if err := counter.DeleteCurrent(); err != nil {
			return err
		}

		// TODO: replace it by Prev(); investigate why it is not working (dupsort.PrevNoDup() works fine)
		k, v, err = counter.Last()
		if err != nil {
			return err
		}
	}

	return nil
}

// Unwind a table whose key is an uint64 (blockNum, ethTx, whatever... we don't care) by
// deleting everything from the end backwards while the key is > specified key.
//
// Usually the unwindTo param is the uunwind point, so its associated record must be preserved,
// hence deleting everything up to unwind point + 1.
func unwindUint64KeyBasedTable(cursor kv.RwCursor, unwindTo uint64) (uint64, error) {
	unwoundToKey := uint64(0)

	k, _, err := cursor.Last()
	if err != nil {
		return 0, err
	}
	for k != nil {
		kAsNum := binary.BigEndian.Uint64(k)
		if kAsNum <= unwindTo {
			unwoundToKey = kAsNum
			break
		}

		if err := cursor.DeleteCurrent(); err != nil {
			return 0, err
		}

		k, _, err = cursor.Prev()
		if err != nil {
			return 0, err
		}
	}

	return unwoundToKey, nil
}

// Unwind a dupsorted table whose key is an uint64 (blockNum, ethTx, whatever... we don't care) by
// deleting everything from the end backwards while the key is > specified key.
//
// Usually the unwindTo param is the uunwind point, so its associated record must be preserved,
// hence deleting everything up to unwind point + 1.
func unwindUint64KeyBasedDupTable(cursor kv.RwCursorDupSort, unwindTo uint64) (uint64, error) {
	unwoundToKey := uint64(0)

	k, _, err := cursor.Last()
	if err != nil {
		return 0, err
	}
	for k != nil {
		kAsNum := binary.BigEndian.Uint64(k)
		if kAsNum <= unwindTo {
			unwoundToKey = kAsNum
			break
		}

		if err := cursor.DeleteCurrentDuplicates(); err != nil {
			return 0, err
		}

		k, _, err = cursor.PrevNoDup()
		if err != nil {
			return 0, err
		}
	}

	return unwoundToKey, nil
}

// Unwind a pair of buckets in the format:
//
// Index table: k: addr+chunkID, v: chunks
// Counter table: k: addr+counter, v: chunkID
func unwindAddress(tx kv.RwTx, target, targetDel kv.RwCursor, counter kv.RwCursorDupSort, indexBucket, counterBucket string, addr common.Address, idx uint64) error {
	key := chunkKey(addr.Bytes(), false, idx)
	k, v, err := target.Seek(key)
	if err != nil {
		return err
	}
	if k == nil || !bytes.HasPrefix(k, addr.Bytes()) {
		// that's ok, because for unwind we take the shortcut and cut everything
		// onwards at the first occurrence, but there may be further occurrences
		// which will just be ignored
		return nil
	}

	// Skip potential rewrites of same chunk due to multiple matches on later blocks
	lastVal := binary.BigEndian.Uint64(v[len(v)-8:])
	if lastVal < idx {
		return nil
	}

	foundChunk := binary.BigEndian.Uint64(k[length.Addr:])
	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)

	for i := 0; i < len(v); i += 8 {
		val := binary.BigEndian.Uint64(v[i : i+8])
		if val >= idx {
			break
		}
		bm.Add(val)
	}

	// Safe copy
	k = slices.Clone(k)
	v = slices.Clone(v)

	// Remove all following chunks
	for {
		if err := targetDel.Delete(k); err != nil {
			return err
		}

		k, _, err = target.Next()
		if err != nil {
			return err
		}
		k = slices.Clone(k)
		if k == nil || !bytes.HasPrefix(k, addr.Bytes()) {
			break
		}
	}

	if !bm.IsEmpty() {
		// Rewrite the found chunk as the last
		newKey := chunkKey(addr.Bytes(), true, 0)
		buf := bytes.NewBuffer(nil)
		b := make([]byte, 8)
		for it := bm.Iterator(); it.HasNext(); {
			val := it.Next()
			binary.BigEndian.PutUint64(b, val)
			buf.Write(b)
		}
		if err := tx.Put(indexBucket, newKey, buf.Bytes()); err != nil {
			return err
		}
	} else {
		// Rewrite the last remaining chunk as the last
		k, v, err := target.Prev()
		if err != nil {
			return err
		}
		k = slices.Clone(k)
		v = slices.Clone(v)
		if k != nil && bytes.HasPrefix(k, addr.Bytes()) {
			if err := targetDel.Delete(k); err != nil {
				return err
			}

			binary.BigEndian.PutUint64(k[length.Addr:], ^uint64(0))
			if err := tx.Put(indexBucket, k, v); err != nil {
				return err
			}
		}
	}

	// Delete counters backwards up to the chunk found
	k, _, err = counter.SeekExact(addr.Bytes())
	if err != nil {
		return err
	}
	k = slices.Clone(k)
	if k == nil {
		return fmt.Errorf("possible db corruption; can't find bucket=%s addr=%s data", counterBucket, addr)
	}

	// Determine if counter is stored in optimized format
	c, err := counter.CountDuplicates()
	if err != nil {
		return err
	}
	v, err = counter.LastDup()
	if err != nil {
		return err
	}
	v = slices.Clone(v)
	isSingleChunkOptimized := c == 1 && len(v) == 1

	// Delete last counter, it'll be replaced on the next step
	lastCounter := uint64(0)
	if isSingleChunkOptimized {
		if err := counter.DeleteCurrent(); err != nil {
			return err
		}
	} else {
		for {
			if len(v) == 1 {
				// DB corrupted
				return fmt.Errorf("db possibly corrupted, len(v) == 1: bucket=%s addr=%s k=%s v=%s", counterBucket, addr, hexutility.Encode(k), hexutility.Encode(v))
			}
			lastCounter = binary.BigEndian.Uint64(v[:length.Counter])
			chunk := binary.BigEndian.Uint64(v[length.Counter:])

			if chunk < foundChunk {
				break
			}
			if err := counter.DeleteCurrent(); err != nil {
				return err
			}
			k, v, err = counter.PrevDup()
			if err != nil {
				return err
			}
			k = slices.Clone(k)
			v = slices.Clone(v)
			if k == nil {
				lastCounter = 0
				isSingleChunkOptimized = true
				break
			}
		}
	}

	// Replace counter
	if !bm.IsEmpty() {
		newCounter := lastCounter + bm.GetCardinality()
		var newValue []byte
		if isSingleChunkOptimized && newCounter <= 256 {
			newValue = indexer.OptimizedCounterSerializer(newCounter)
		} else {
			newValue = indexer.LastCounterSerializer(newCounter)
		}
		if err := tx.Put(counterBucket, addr.Bytes(), newValue); err != nil {
			return err
		}
	} else {
		// Rewrite previous counter (if it exists) pointing it to last chunk
		if k != nil && !isSingleChunkOptimized {
			if err := counter.DeleteCurrent(); err != nil {
				return err
			}

			binary.BigEndian.PutUint64(v[length.Counter:], ^uint64(0))
			if err := tx.Put(counterBucket, addr.Bytes(), v); err != nil {
				return err
			}
		}
	}

	return nil
}
