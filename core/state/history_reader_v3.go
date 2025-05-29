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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
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
func (hr *HistoryReaderV3) SetTx(tx kv.TemporalTx) { hr.ttx = tx }
func (hr *HistoryReaderV3) SetTxNum(txNum uint64)  { hr.txNum = txNum }
func (hr *HistoryReaderV3) GetTxNum() uint64       { return hr.txNum }
func (hr *HistoryReaderV3) SetTrace(trace bool)    { hr.trace = trace }

// Gets the txNum where Account, Storage and Code history begins.
// If the node is an archive node all history will be available therefore
// the result will be 0.
//
// For non-archive node old history files get deleted, so this number will vary
// but the goal is to know where the historical data begins.
func (hr *HistoryReaderV3) StateHistoryStartFrom() uint64 {
	var earliestTxNum uint64 = 0
	// get the first txnum where  accounts, storage , and code are all available in history files
	// This is max(HistoryStart(Accounts), HistoryStart(Storage), HistoryStart(Code))
	stateDomainNames := []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain}
	for _, domainName := range stateDomainNames {
		domainStartingTxNum := hr.ttx.HistoryStartFrom(domainName)
		if domainStartingTxNum > earliestTxNum {
			earliestTxNum = domainStartingTxNum
		}
	}
	return earliestTxNum
}

func (hr *HistoryReaderV3) ReadSet() map[string]*state.KvList { return nil }
func (hr *HistoryReaderV3) ResetReadSet()                     {}
func (hr *HistoryReaderV3) DiscardReadList()                  {}

func (hr *HistoryReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, ok, err := hr.ttx.GetAsOf(kv.AccountsDomain, address[:], hr.txNum)
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
func (hr *HistoryReaderV3) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	return hr.ReadAccountData(address)
}

func (hr *HistoryReaderV3) ReadAccountStorage(address common.Address, key common.Hash) ([]byte, error) {
	hr.composite = append(append(hr.composite[:0], address[:]...), key[:]...)
	enc, _, err := hr.ttx.GetAsOf(kv.StorageDomain, hr.composite, hr.txNum)
	if hr.trace {
		fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, key, enc)
	}
	return enc, err
}

func (hr *HistoryReaderV3) ReadAccountCode(address common.Address) ([]byte, error) {
	//  must pass key2=Nil here: because Erigon4 does concatinate key1+key2 under the hood
	//code, _, err := hr.ttx.GetAsOf(kv.CodeDomain, address.Bytes(), codeHash.Bytes(), hr.txNum)
	code, _, err := hr.ttx.GetAsOf(kv.CodeDomain, address[:], hr.txNum)
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x] => [%x]\n", address, code)
	}
	return code, err
}

func (hr *HistoryReaderV3) ReadAccountCodeSize(address common.Address) (int, error) {
	enc, _, err := hr.ttx.GetAsOf(kv.CodeDomain, address[:], hr.txNum)
	return len(enc), err
}

func (hr *HistoryReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	enc, ok, err := hr.ttx.GetAsOf(kv.AccountsDomain, address.Bytes(), hr.txNum)
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

type ResettableStateReader interface {
	StateReader
	SetTx(tx kv.TemporalTx)
	SetTxNum(txn uint64)
	DiscardReadList()
	ReadSet() map[string]*state.KvList
	ResetReadSet()
}
