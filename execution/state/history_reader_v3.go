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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var PrunedError = errors.New("old data not available due to pruning")

// HistoryReaderV3 Implements StateReader and StateWriter
type HistoryReaderV3 struct {
	txNum     uint64
	trace     bool
	ttx       kv.TemporalTx
	composite []byte
}

func NewHistoryReaderV3() *HistoryReaderV3 {
	return &HistoryReaderV3{composite: make([]byte, 20+32)}
}

func (hr *HistoryReaderV3) String() string {
	return fmt.Sprintf("txNum:%d", hr.txNum)
}
func (hr *HistoryReaderV3) SetTx(tx kv.TemporalTx)                  { hr.ttx = tx }
func (hr *HistoryReaderV3) SetTxNum(txNum uint64)                   { hr.txNum = txNum }
func (hr *HistoryReaderV3) GetTxNum() uint64                        { return hr.txNum }
func (hr *HistoryReaderV3) SetTrace(trace bool, tracePrefix string) { hr.trace = trace }

// Gets the txNum where Account, Storage and Code history begins.
// If the node is an archive node all history will be available therefore
// the result will be 0.
//
// For non-archive node old history files get deleted, so this number will vary
// but the goal is to know where the historical data begins.
func (hr *HistoryReaderV3) StateHistoryStartFrom() uint64 {
	dbg := hr.ttx.Debug()
	return min(
		dbg.HistoryStartFrom(kv.AccountsDomain),
		dbg.HistoryStartFrom(kv.StorageDomain),
		dbg.HistoryStartFrom(kv.CodeDomain),
	)
}

func (hr *HistoryReaderV3) DiscardReadList() {}

func (hr *HistoryReaderV3) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addressValue := address.Value()
	enc, ok, err := hr.ttx.GetAsOf(kv.AccountsDomain, addressValue[:], hr.txNum)
	if err != nil || !ok || len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => []\n", address)
		}
		return nil, err
	}
	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
	}
	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
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
	enc, ok, err := hr.ttx.GetAsOf(kv.StorageDomain, hr.composite, hr.txNum)
	if hr.trace {
		fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, key, enc)
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
	code, _, err := hr.ttx.GetAsOf(kv.CodeDomain, addressValue[:], hr.txNum)
	if hr.trace {
		lenc, cs := printCode(code)
		fmt.Printf("ReadAccountCode [%x] => [%d:%s]\n", address, lenc, cs)
	}
	return code, err
}

func (hr *HistoryReaderV3) ReadAccountCodeSize(address accounts.Address) (int, error) {
	addressValue := address.Value()
	enc, _, err := hr.ttx.GetAsOf(kv.CodeDomain, addressValue[:], hr.txNum)
	return len(enc), err
}

func (hr *HistoryReaderV3) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	addressValue := address.Value()
	enc, ok, err := hr.ttx.GetAsOf(kv.AccountsDomain, addressValue[:], hr.txNum)
	if err != nil || !ok || len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountIncarnation [%x] => [0]\n", address)
		}
		return 0, err
	}
	var a accounts.Account
	if err := a.DecodeForStorage(enc); err != nil {
		return 0, fmt.Errorf("ReadAccountIncarnation(%x): %w", address, err)
	}
	if a.Incarnation == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountIncarnation [%x] => [%d]\n", address, 0)
		}
		return 0, nil
	}
	if hr.trace {
		fmt.Printf("ReadAccountIncarnation [%x] => [%d]\n", address, a.Incarnation-1)
	}
	return a.Incarnation - 1, nil
}
