// Copyright 2024 The Erigon Authors
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

package state

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var PrunedError = errors.New("old data not available due to pruning")

// HistoryReaderV3 Implements StateReader and StateWriter.
//
// The read chain, from most-recent to persisted, is:
//
//	blockCache (in-flight parallel-block writes, per-field) →
//	sd.GetAsOf (in-batch memory state) →
//	ttx.GetAsOf (DB history + snapshot files).
//
// blockCache is populated by applyVersionedWrites in the parallel executor
// for every committed tx in the current block. Those writes do NOT land in
// sd.mem until Flush at block boundary, so a finalize-time IBS constructed
// in historic mode (withdrawals, EIP-7002/7251 system calls on a
// tip-adjacent historic block) would otherwise read the pre-block balance
// and stomp a prior tx's in-block update. Consulting blockCache first fixes
// that gap.
//
// sd is the SharedDomains for the batch. When non-nil, the reader checks
// sd.GetAsOf before ttx.GetAsOf so prior-batch writes that haven't been
// flushed to the history index yet are still visible.
//
// RPC and other consumers that want to read strictly persisted history
// pass sd=nil and blockCache=nil via the legacy NewHistoryReaderV3
// constructor.
type HistoryReaderV3 struct {
	txNum       uint64
	trace       bool
	tracePrefix string
	ttx         kv.TemporalTx
	sd          *execctx.SharedDomains
	blockCache  *BlockStateCache
	composite   []byte
}

func NewHistoryReaderV3(ttx kv.TemporalTx, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{composite: make([]byte, 20+32), ttx: ttx, txNum: txNum}
}

// NewHistoryReaderV3WithSharedDomains is the in-batch variant used by the
// parallel executor. Reads chain sd.GetAsOf (in-memory batch state) then
// fall back to ttx.GetAsOf so a tx can see prior-tx writes from the same
// batch that have not yet been flushed to the history index.
func NewHistoryReaderV3WithSharedDomains(ttx kv.TemporalTx, sd *execctx.SharedDomains, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{composite: make([]byte, 20+32), ttx: ttx, sd: sd, txNum: txNum}
}

// NewHistoryReaderV3WithBlockCache is the finalize-time variant used by
// the parallel executor for historic blocks. In addition to the
// sd.GetAsOf → ttx.GetAsOf chain, it consults the per-block BlockStateCache
// first so the block-finalize IBS (withdrawals, EIP-7002/7251 system calls)
// sees every prior-tx write recorded in the current block, not just the
// pre-block committed state.
func NewHistoryReaderV3WithBlockCache(ttx kv.TemporalTx, sd *execctx.SharedDomains, blockCache *BlockStateCache, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{composite: make([]byte, 20+32), ttx: ttx, sd: sd, blockCache: blockCache, txNum: txNum}
}

// SetBlockStateCache updates the in-flight block cache tier. Used when the
// reader is reused across blocks in the parallel executor.
func (hr *HistoryReaderV3) SetBlockStateCache(cache *BlockStateCache) {
	hr.blockCache = cache
}

// getAsOf chains blockCache (in-flight parallel block writes, per-field)
// before sd.GetAsOf (in-batch memory) and ttx.GetAsOf (DB history +
// snapshot files). Callers requesting only persisted history construct the
// reader with sd=nil and blockCache=nil. If sd.mem has inMemHistoryReads
// disabled (e.g. the serial executor path), sd.GetAsOf returns an error —
// we silently fall through to ttx so the same reader type is usable in
// both modes.
//
// blockCache is consulted for the AccountsDomain and StorageDomain only;
// CodeDomain is not currently cached per block, so for code reads we fall
// straight through to sd/ttx. For storage we use the full 52-byte
// composite key (addr||slot) like the rest of the state domain.
func (hr *HistoryReaderV3) getAsOf(domain kv.Domain, key []byte) (enc []byte, ok bool, err error) {
	if hr.blockCache != nil {
		switch domain {
		case kv.AccountsDomain:
			if len(key) == 20 {
				var raw common.Address
				copy(raw[:], key)
				addr := accounts.InternAddress(raw)
				if cached, hit := hr.blockCache.GetCurrentAccount(addr); hit {
					// hit==true is authoritative for the in-flight block, including the
					// deletion case (cached==nil). Return immediately rather than falling
					// through to sd/ttx, which would surface the pre-deletion value from
					// history. Downstream callers (ReadAccountData) treat enc=nil as
					// "no account" regardless of ok, so emitting ok=true here just
					// reflects the cache's authoritative status.
					if cached == nil {
						return nil, false, nil
					}
					return cached, true, nil
				}
			}
		case kv.StorageDomain:
			if len(key) == 20+32 {
				var rawAddr common.Address
				var rawSlot common.Hash
				copy(rawAddr[:], key[:20])
				copy(rawSlot[:], key[20:])
				addr := accounts.InternAddress(rawAddr)
				slot := accounts.InternKey(rawSlot)
				if cached, hit := hr.blockCache.GetCurrentStorage(addr, slot); hit {
					// Same as the account case above: hit==true is authoritative even
					// when the slot was cleared (len(cached)==0), so do not fall through.
					if len(cached) == 0 {
						return nil, false, nil
					}
					return cached, true, nil
				}
			}
		}
	}
	if hr.sd != nil {
		enc, ok, err = hr.sd.GetAsOf(domain, key, hr.txNum)
		if err == nil && ok {
			return enc, true, nil
		}
	}
	return hr.ttx.GetAsOf(domain, key, hr.txNum)
}

func (hr *HistoryReaderV3) String() string {
	return fmt.Sprintf("txNum:%d", hr.txNum)
}
func (hr *HistoryReaderV3) SetTx(tx kv.TemporalTx) { hr.ttx = tx }
func (hr *HistoryReaderV3) SetTxNum(txNum uint64)  { hr.txNum = txNum }
func (hr *HistoryReaderV3) GetTxNum() uint64       { return hr.txNum }
func (hr *HistoryReaderV3) SetTrace(trace bool, tracePrefix string) {
	hr.trace = trace
	hr.tracePrefix = tracePrefix
}

func (r *HistoryReaderV3) Trace() bool {
	return r.trace
}

func (r *HistoryReaderV3) TracePrefix() string {
	return r.tracePrefix
}

// Gets the txNum where Account, Storage and Code history begins.
// If the node is an archive node all history will be available therefore
// the result will be 0.
//
// For non-archive node old history files get deleted, so this number will vary
// but the goal is to know where the historical data begins.
func StateHistoryStartTxNum(ttx kv.TemporalTx) uint64 {
	dbg := ttx.Debug()
	return min(
		dbg.HistoryStartFrom(kv.AccountsDomain),
		dbg.HistoryStartFrom(kv.StorageDomain),
		dbg.HistoryStartFrom(kv.CodeDomain),
	)
}

func (hr *HistoryReaderV3) DiscardReadList() {}

func (hr *HistoryReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addressValue := address.Value()
	enc, ok, err := hr.getAsOf(kv.AccountsDomain, addressValue[:])
	if err != nil || !ok || len(enc) == 0 {
		if hr.trace {
			fmt.Printf("%sReadAccountData (hist)[%x] => []\n", hr.tracePrefix, address)
		}
		return nil, err
	}
	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, fmt.Errorf("%sread account data (hist)(%x): %w", hr.tracePrefix, address, err)
	}
	if hr.trace {
		fmt.Printf("%sReadAccountData (hist)[%x] => [nonce: %d, balance: %d, codeHash: %x]\n", hr.tracePrefix, address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

// ReadAccountDataForDebug - is like ReadAccountData, but without adding key to `readList`.
// Used to get `prev` account balance
func (hr *HistoryReaderV3) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return hr.ReadAccountData(address)
}

func (hr *HistoryReaderV3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	keyValue := key.Value()
	hr.composite = append(append(hr.composite[:0], addressValue[:]...), keyValue[:]...)
	enc, ok, err := hr.getAsOf(kv.StorageDomain, hr.composite)
	if hr.trace {
		fmt.Printf("%sReadAccountStorage (hist)[%x] [%x] => [%x]\n", hr.tracePrefix, address, key, enc)
	}
	var res uint256.Int
	if ok {
		(&res).SetBytes(enc)
	}
	return res, ok, err
}

func (hr *HistoryReaderV3) HasStorage(address accounts.Address) (bool, error) {
	addressValue := address.Value()
	to, ok := kv.NextSubtree(addressValue[:])
	if !ok {
		to = nil
	}

	it, err := hr.ttx.RangeAsOf(kv.StorageDomain, addressValue[:], to, hr.txNum, order.Asc, kv.Unlim)
	if err != nil {
		return false, err
	}

	defer it.Close()
	// Note: if a storage for an address gets deleted, the historical RangeAsOf will return its slots as empty values.
	// If the address doesn't have any storage slots, then we return "no storage" immediately
	// If the address has storage slots, but they are all empty, then we return "no storage"
	// If we see a non-empty slot for then address, then we return "has storage" immediately
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return false, err
		}

		if len(v) != 0 {
			return true, nil
		}
	}

	return false, nil
}

func (hr *HistoryReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	//  must pass key2=Nil here: because Erigon4 does concatinate key1+key2 under the hood
	//code, _, err := hr.ttx.GetAsOf(kv.CodeDomain, address.Bytes(), codeHash.Bytes(), hr.txNum)
	addressValue := address.Value()
	code, _, err := hr.getAsOf(kv.CodeDomain, addressValue[:])
	if hr.trace {
		lenc, cs := printCode(code)
		fmt.Printf("%sReadAccountCode (hist)[%x] => [%d:%s]\n", hr.tracePrefix, address, lenc, cs)
	}
	return code, err
}

func (hr *HistoryReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	addressValue := address.Value()
	enc, _, err := hr.getAsOf(kv.CodeDomain, addressValue[:])
	return len(enc), err
}

func (hr *HistoryReaderV3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	addressValue := address.Value()
	enc, ok, err := hr.getAsOf(kv.AccountsDomain, addressValue[:])
	if err != nil || !ok || len(enc) == 0 {
		if hr.trace {
			fmt.Printf("%sReadAccountIncarnation (hist)[%x] => [0]\n", hr.tracePrefix, address)
		}
		return 0, err
	}
	var a accounts.Account
	if err := a.DecodeForStorage(enc); err != nil {
		return 0, fmt.Errorf("%sread account incarnation (hist)[%x]: %w", hr.tracePrefix, address, err)
	}
	if a.Incarnation == 0 {
		if hr.trace {
			fmt.Printf("%sReadAccountIncarnation (hist)[%x] => [%d]\n", hr.tracePrefix, address, 0)
		}
		return 0, nil
	}
	if hr.trace {
		fmt.Printf("%sReadAccountIncarnation (hist)[%x] => [%d]\n", hr.tracePrefix, address, a.Incarnation-1)
	}
	return a.Incarnation - 1, nil
}
