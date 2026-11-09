package rpchelper

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// shortcut: entries never expire, so reorgs, pruning and a moving head are ignored; only valid as a bound on a fixed-block benchmark.
var (
	callHeaderCacheOn = os.Getenv("CALL_HEADER_CACHE") == "1"
	callReaderReuseOn = os.Getenv("CALL_READER_REUSE") == "1"

	callHeaders    sync.Map // callHeaderKey -> cachedCallHeader
	callHeadersLen atomic.Int32
	callReaders    sync.Map // block number -> *callReaderEntry
	callReadersLen atomic.Int32
)

const callProbeMaxEntries = 1024

func storeCapped(m *sync.Map, size *atomic.Int32, key, value any) {
	if size.Load() >= callProbeMaxEntries {
		return
	}
	if _, loaded := m.LoadOrStore(key, value); !loaded {
		size.Add(1)
	}
}

type callHeaderKey struct {
	number rpc.BlockNumber
	hash   common.Hash
	byHash bool
}

func newCallHeaderKey(selector rpc.BlockNumberOrHash) callHeaderKey {
	if hash, ok := selector.Hash(); ok {
		return callHeaderKey{hash: hash, byHash: true}
	}
	number, _ := selector.Number()
	return callHeaderKey{number: number}
}

type cachedCallHeader struct {
	header   *types.Header
	isLatest bool
}

func CachedCallHeader(selector rpc.BlockNumberOrHash) (header *types.Header, isLatest bool, ok bool) {
	if !callHeaderCacheOn {
		return nil, false, false
	}
	v, ok := callHeaders.Load(newCallHeaderKey(selector))
	if !ok {
		return nil, false, false
	}
	cached := v.(cachedCallHeader)
	return cached.header, cached.isLatest, true
}

func StoreCallHeader(selector rpc.BlockNumberOrHash, header *types.Header, isLatest bool) {
	if callHeaderCacheOn {
		storeCapped(&callHeaders, &callHeadersLen, newCallHeaderKey(selector), cachedCallHeader{header: header, isLatest: isLatest})
	}
}

// callReaderEntry keeps what a historical reader needs per block. The view may outlive
// the call that made it because GetAsOf of every kvcache view ignores its tx.
type callReaderEntry struct {
	cache asOfView
	txNum uint64
	pool  sync.Pool
}

func (e *callReaderEntry) acquire(tx kv.TemporalTx) *cachedHistoryReaderV3 {
	r, ok := e.pool.Get().(*cachedHistoryReaderV3)
	if !ok {
		return &cachedHistoryReaderV3{
			cache:     e.cache,
			reader:    state.NewHistoryReaderV3(tx, e.txNum),
			composite: make([]byte, 0, len(common.Address{})+len(common.Hash{})),
		}
	}
	r.reader.SetTx(tx)
	r.reader.SetTxNum(e.txNum)
	return r
}

// CallStateReader is CreateStateReaderFromBlockNumber for eth_call. With CALL_READER_REUSE=1
// a historical reader is rebound to tx from a per-block pool.
func CallStateReader(ctx context.Context, tx kv.TemporalTx, blockNumber uint64, latest bool, stateCache kvcache.Cache, txNumsReader rawdbv3.TxNumsReader) (state.StateReader, error) {
	if callReaderReuseOn && !latest {
		if v, ok := callReaders.Load(blockNumber); ok {
			return v.(*callReaderEntry).acquire(tx), nil
		}
	}
	reader, err := CreateStateReaderFromBlockNumber(ctx, tx, blockNumber, latest, 0, stateCache, txNumsReader)
	if err != nil {
		return nil, err
	}
	if history, ok := reader.(*cachedHistoryReaderV3); ok && callReaderReuseOn {
		storeCapped(&callReaders, &callReadersLen, blockNumber, &callReaderEntry{cache: history.cache, txNum: history.GetTxNum()})
	}
	return reader, nil
}

// ReleaseCallStateReader returns reader to its pool; the caller must not read from it afterwards.
func ReleaseCallStateReader(blockNumber uint64, reader state.StateReader) {
	if !callReaderReuseOn {
		return
	}
	history, ok := reader.(*cachedHistoryReaderV3)
	if !ok {
		return
	}
	if v, ok := callReaders.Load(blockNumber); ok {
		history.reader.SetTx(nil)
		v.(*callReaderEntry).pool.Put(history)
	}
}
