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
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var PrunedError = errors.New("old data not available due to pruning")

// HistoryReaderV3 implements StateReader and StateWriter. Reads chain from
// most-recent to persisted: blockCache (in-flight per-field parallel-block
// writes) → sd.GetAsOf (in-batch memory) → ttx.GetAsOf (DB history + snapshots).
//
// blockCache holds writes that have not yet reached sd.mem until the block
// boundary Flush, so a finalize-time IBS in historic mode would otherwise read
// pre-block state and stomp a prior tx's in-block update. When sd is nil the
// reader serves strictly persisted history.
type HistoryReaderV3 struct {
	ttx         kv.TemporalTx
	sd          execctx.DomainReader
	tracePrefix string
	txNum       uint64
	composite   [length.Addr + length.Hash]byte // reused storage lookup key (addr||slot)
	addr        common.Address                  // reused account/code lookup key
	trace       bool
}

func NewHistoryReaderV3(ttx kv.TemporalTx, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{ttx: ttx, txNum: txNum}
}

// NewHistoryReaderV3WithSharedDomains is the in-batch variant used by the
// parallel executor. Reads chain sd.GetAsOf (in-memory batch state) then
// fall back to ttx.GetAsOf so a tx can see prior-tx writes from the same
// batch that have not yet been flushed to the history index.
func NewHistoryReaderV3WithSharedDomains(ttx kv.TemporalTx, sd execctx.DomainReader, txNum uint64) *HistoryReaderV3 {
	return &HistoryReaderV3{ttx: ttx, sd: sd, txNum: txNum}
}

// getAsOf chains sd.GetAsOf (in-batch memory) before ttx.GetAsOf (DB history
// + snapshots). When sd is nil, or sd.mem has inMemHistoryReads disabled and
// returns an error, it falls through to ttx so the same reader type serves
// both the in-batch and persisted-history modes.
func (hr *HistoryReaderV3) getAsOf(domain kv.Domain, key []byte) (enc []byte, ok bool, err error) {
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

// ReadAccountDataForDebug - is like ReadAccountData, but without adding key to `readList`.
// Used to get `prev` account balance
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

func (hr *HistoryReaderV3) ReadAccountCode(address accounts.Address) ([]byte, error) {
	//  must pass key2=Nil here: because Erigon4 does concatinate key1+key2 under the hood
	//code, _, err := hr.ttx.GetAsOf(kv.CodeDomain, address.Bytes(), codeHash.Bytes(), hr.txNum)
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
