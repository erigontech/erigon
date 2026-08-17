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
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var PrunedError = errors.New("old data not available due to pruning")

// HistoryReaderV3 implements StateReader/StateWriter, reading blockCache
// (in-flight per-block writes) before sd.GetAsOf (unflushed in-batch state)
// before ttx.GetAsOf (persisted history): a finalize-time historic read would
// otherwise see the pre-block value and stomp an earlier tx's write.
type HistoryReaderV3 struct {
	ttx         kv.TemporalTx
	sd          *execctx.SharedDomains
	blockCache  *BlockStateCache
	tracePrefix string
	txNum       uint64
	composite   [length.Addr + length.Hash]byte // reused storage lookup key (addr||slot)
	addr        common.Address                  // reused account/code lookup key
	trace       bool
}

func NewHistoryReaderV3(ttx kv.TemporalTx, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{ttx: ttx, txNum: txNum}
}

func NewHistoryReaderV3WithSharedDomains(ttx kv.TemporalTx, sd *execctx.SharedDomains, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{ttx: ttx, sd: sd, txNum: txNum}
}

func NewHistoryReaderV3WithBlockCache(ttx kv.TemporalTx, sd *execctx.SharedDomains, blockCache *BlockStateCache, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{ttx: ttx, sd: sd, blockCache: blockCache, txNum: txNum}
}

func (hr *HistoryReaderV3) SetBlockStateCache(cache *BlockStateCache) {
	hr.blockCache = cache
}

// blockCache covers Accounts/Storage only, not Code. sd.GetAsOf errors (e.g.
// history reads disabled) fall through to ttx rather than failing the read.
func (hr *HistoryReaderV3) getAsOf(domain kv.Domain, key []byte) (enc []byte, ok bool, err error) {
	if hr.blockCache != nil {
		switch domain {
		case kv.AccountsDomain:
			if len(key) == 20 {
				var raw common.Address
				copy(raw[:], key)
				addr := accounts.InternAddress(raw)
				if cached, hit := hr.blockCache.GetCurrentAccount(addr); hit {
					// hit==true is authoritative even when cached==nil (deleted): return
					// now, don't fall through to sd/ttx and resurrect the pre-deletion value.
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
					// Same as the account case: authoritative even when cleared (len==0).
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
	hr.addr = address.Value()
	enc, ok, err := hr.getAsOf(kv.AccountsDomain, hr.addr[:])
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
		fmt.Printf("%sReadAccountData (hist)[%x] => [nonce: %d, balance: %s, codeHash: %x]\n", hr.tracePrefix, address, a.Nonce, a.Balance.String(), a.CodeHash)
	}
	return &a, nil
}

func (hr *HistoryReaderV3) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return hr.ReadAccountData(address)
}

func (hr *HistoryReaderV3) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	keyValue := key.Value()
	copy(hr.composite[:length.Addr], addressValue[:])
	copy(hr.composite[length.Addr:], keyValue[:])
	enc, ok, err := hr.getAsOf(kv.StorageDomain, hr.composite[:])
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
	hr.addr = address.Value()
	to, ok := kv.NextSubtree(hr.addr[:])
	if !ok {
		to = nil
	}

	it, err := hr.ttx.RangeAsOf(kv.StorageDomain, hr.addr[:], to, hr.txNum, order.Asc, kv.Unlim)
	if err != nil {
		return false, err
	}

	defer it.Close()
	// A deleted storage slot surfaces as an empty value in RangeAsOf's history, not as an absent key.
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
	// CodeDomain keys are address-only; do not append codeHash.
	hr.addr = address.Value()
	code, _, err := hr.getAsOf(kv.CodeDomain, hr.addr[:])
	if hr.trace {
		lenc, cs := printCode(code)
		fmt.Printf("%sReadAccountCode (hist)[%x] => [%d:%s]\n", hr.tracePrefix, address, lenc, cs)
	}
	return code, err
}

func (hr *HistoryReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	hr.addr = address.Value()
	enc, _, err := hr.getAsOf(kv.CodeDomain, hr.addr[:])
	return len(enc), err
}

func (hr *HistoryReaderV3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	hr.addr = address.Value()
	enc, ok, err := hr.getAsOf(kv.AccountsDomain, hr.addr[:])
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
