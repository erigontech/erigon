package jsonrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
)

type SearchResultMaterializer[T any] interface {
	Convert(ctx context.Context, tx kv.Tx, idx uint64) (*T, error)
}

func genericResultList[T any](ctx context.Context, tx kv.Tx, addr common.Address, idx, count uint64, indexBucket, counterBucket string, srm SearchResultMaterializer[T]) ([]*T, error) {
	chunks, err := tx.Cursor(indexBucket)
	if err != nil {
		return nil, err
	}
	defer chunks.Close()

	counter, err := tx.CursorDupSort(counterBucket)
	if err != nil {
		return nil, err
	}
	defer counter.Close()

	// Determine page [start, end]
	startIdx := idx + 1

	// Locate first chunk
	counterFound, chunk, err := findNextCounter(counter, addr, startIdx)
	if err != nil {
		return nil, err
	}
	if counterFound == 0 {
		return []*T{}, nil
	}

	// Locate first chunk
	chunkKey := make([]byte, length.Addr+length.Chunk)
	copy(chunkKey, addr.Bytes())
	copy(chunkKey[length.Addr:], chunk)
	k, v, err := chunks.SeekExact(chunkKey)
	if err != nil {
		return nil, err
	}
	if k == nil {
		return nil, fmt.Errorf("db possibly corrupted, couldn't find chunkKey %s on bucket %s", hexutility.Encode(chunkKey), indexBucket)
	}

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)

	for i := 0; i < len(v); i += 8 {
		bm.Add(binary.BigEndian.Uint64(v[i : i+8]))
	}

	// bitmap contains idxs [counterFound - bm.GetCardinality(), counterFound - 1]
	startCounter := counterFound - bm.GetCardinality() + 1
	if startIdx > startCounter {
		endRange, err := bm.Select(startIdx - startCounter - 1)
		if err != nil {
			return nil, err
		}
		bm.RemoveRange(bm.Minimum(), endRange+1)
	}

	ret := make([]*T, 0)
	it := bm.Iterator()
	for c := uint64(0); c < count; c++ {
		// Look at next chunk?
		if !it.HasNext() {
			k, v, err = chunks.Next()
			if err != nil {
				return nil, err
			}
			if !bytes.HasPrefix(k, addr.Bytes()) {
				break
			}

			bm.Clear()
			for i := 0; i < len(v); i += 8 {
				bm.Add(binary.BigEndian.Uint64(v[i : i+8]))
			}
			it = bm.Iterator()
		}

		// Convert match ID to proper struct data (it maybe a tx, a withdraw, etc, determined by T)
		idx := it.Next()
		result, err := srm.Convert(ctx, tx, idx)
		if err != nil {
			return nil, err
		}
		ret = append(ret, result)
	}

	return ret, nil
}

// Given an index, locates the counter chunk which should contain the desired index (>= index)
func findNextCounter(counter kv.CursorDupSort, addr common.Address, idx uint64) (uint64, []byte, error) {
	v, err := counter.SeekBothRange(addr.Bytes(), hexutility.EncodeTs(idx))
	if err != nil {
		return 0, nil, err
	}

	// No occurrences
	if v == nil {
		return 0, nil, nil
	}

	// <= 256 matches-optimization
	if len(v) == 1 {
		c, err := counter.CountDuplicates()
		if err != nil {
			return 0, nil, err
		}
		if c != 1 {
			return 0, nil, fmt.Errorf("db possibly corrupted, expected 1 duplicate, got %d", c)
		}
		return uint64(v[0]) + 1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, nil
	}

	// Regular chunk
	return binary.BigEndian.Uint64(v[:length.Ts]), v[length.Ts:], nil
}

func (api *Otterscan2APIImpl) genericGetCount(ctx context.Context, addr common.Address, counterBucket string) (uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	counter, err := tx.CursorDupSort(counterBucket)
	if err != nil {
		return 0, err
	}
	defer counter.Close()

	k, _, err := counter.SeekExact(addr.Bytes())
	if err != nil {
		return 0, err
	}
	if k == nil {
		return 0, nil
	}

	v, err := counter.LastDup()
	if err != nil {
		return 0, err
	}

	// Check if it is in the <= 256 count optimization
	if len(v) == 1 {
		c, err := counter.CountDuplicates()
		if err != nil {
			return 0, err
		}
		if c != 1 {
			return 0, fmt.Errorf("db possibly corrupted, expected 1 duplicate, got %d", c)
		}

		return uint64(v[0]) + 1, nil
	}

	return binary.BigEndian.Uint64(v[:8]), nil
}
