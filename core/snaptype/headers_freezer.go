package snaptype

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"time"

	"github.com/erigontech/erigon-lib/common"
	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/crypto/cryptopool"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/state"
	ee "github.com/erigontech/erigon-lib/state/appendable_extras"
	"github.com/erigontech/erigon/core/types"
)

type HeaderFreezer struct {
	canonicalTbl, valsTbl string
	coll                  state.Collector
	logger                log.Logger
}

var _ state.Freezer = (*HeaderFreezer)(nil)

func NewHeaderFreezer(canonicalTbl, valsTbl string, logger log.Logger) *HeaderFreezer {
	return &HeaderFreezer{canonicalTbl, valsTbl, nil, logger}
}

func (f *HeaderFreezer) Freeze(ctx context.Context, blockFrom, blockTo ee.RootNum, db kv.RoDB) error {
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
			dbg.ReadMemStats(&m)
			f.logger.Info("[snapshots] Dumping headers", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	})
}

func (f *HeaderFreezer) SetCollector(coll state.Collector) {
	f.coll = coll
}

var _ state.IndexKeyFactory = (*HeaderAccessorIndexKeyFactory)(nil)

type HeaderAccessorIndexKeyFactory struct {
	s crypto.KeccakState
	h common.Hash
}

func (f *HeaderAccessorIndexKeyFactory) Refresh() {
	f.s = crypto.NewKeccakState()
}

func (f *HeaderAccessorIndexKeyFactory) Make(word []byte, _ uint64) []byte {
	headerRlp := word[1:]
	f.s.Reset()
	f.s.Write(headerRlp)
	f.s.Read(f.h[:])
	return f.h[:]
}

func (f *HeaderAccessorIndexKeyFactory) Close() {
	cryptopool.ReturnToPoolKeccak256(f.s)
}
