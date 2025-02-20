package snaptype

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/state"
	ae "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon/core/types"
)

// type Freezer interface {
// 	// baseNumFrom/To represent num which the snapshot should range
// 	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
// 	// to ensure this.
// 	Freeze(ctx context.Context, from, to RootNum, db kv.Db) error
// 	SetCollector(coll Collector)
// }

type HeaderFreezer struct {
	canonicalTbl, valsTbl string
	coll                  state.Collector
}

func (f *HeaderFreezer) Freeze(ctx context.Context, blockFrom, blockTo ae.RootNum, db kv.RoDB) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	key := make([]byte, 8+32)
	from := hexutil.EncodeTs(uint64(blockFrom))
	return kv.BigChunks(db, f.canonicalTbl, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= uint64(blockTo) {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)
		dataRLP, err := tx.GetOne(f.valsTbl, key)
		if err != nil {
			return false, err
		}
		if dataRLP == nil {
			return false, fmt.Errorf("header missed in db: block_num=%d,  hash=%x", blockNum, v)
		}
		h := types.Header{}
		if err := rlp.DecodeBytes(dataRLP, &h); err != nil {
			return false, err
		}

		value := make([]byte, len(dataRLP)+1) // first_byte_of_header_hash + header_rlp
		value[0] = h.Hash()[0]
		copy(value[1:], dataRLP)
		if err := f.coll(value); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Dumping headers", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	})
}

func (h *HeaderFreezer) SetCollector(coll state.Collector) {
	h.coll = coll
}
