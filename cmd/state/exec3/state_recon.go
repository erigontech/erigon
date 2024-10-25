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

package exec3

import (
	"encoding/binary"

	"github.com/RoaringBitmap/roaring/roaring64"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/etl"
	libstate "github.com/erigontech/erigon-lib/state"

	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types/accounts"
)

type ScanWorker struct {
	txNum  uint64
	as     *libstate.AggregatorStep
	toKey  []byte
	bitmap roaring64.Bitmap
}

func NewScanWorker(txNum uint64, as *libstate.AggregatorStep) *ScanWorker {
	sw := &ScanWorker{
		txNum: txNum,
		as:    as,
	}
	return sw
}

type FillWorker struct {
	txNum uint64
	as    *libstate.AggregatorStep
}

func NewFillWorker(txNum uint64, as *libstate.AggregatorStep) *FillWorker {
	fw := &FillWorker{
		txNum: txNum,
		as:    as,
	}
	return fw
}

func (fw *FillWorker) FillAccounts(plainStateCollector *etl.Collector) error {
	it := fw.as.IterateAccountsHistory(fw.txNum)
	value := make([]byte, 1024)
	for it.HasNext() {
		key, val, err := it.Next()
		if err != nil {
			return err
		}
		if len(val) > 0 {
			var a accounts.Account
			//if err:=accounts.DeserialiseV3(&a, val);err!=nil {
			//	panic(err)
			//}
			a.Reset()
			pos := 0
			nonceBytes := int(val[pos])
			pos++
			if nonceBytes > 0 {
				a.Nonce = bytesToUint64(val[pos : pos+nonceBytes])
				pos += nonceBytes
			}
			balanceBytes := int(val[pos])
			pos++
			if balanceBytes > 0 {
				a.Balance.SetBytes(val[pos : pos+balanceBytes])
				pos += balanceBytes
			}
			codeHashBytes := int(val[pos])
			pos++
			if codeHashBytes > 0 {
				copy(a.CodeHash[:], val[pos:pos+codeHashBytes])
				pos += codeHashBytes
			}
			incBytes := int(val[pos])
			pos++
			if incBytes > 0 {
				a.Incarnation = bytesToUint64(val[pos : pos+incBytes])
			}
			if a.Incarnation > 0 {
				a.Incarnation = state.FirstContractIncarnation
			}
			value = value[:a.EncodingLengthForStorage()]
			a.EncodeForStorage(value)
			if err := plainStateCollector.Collect(key, value); err != nil {
				return err
			}
			//fmt.Printf("Account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", key, &a.Balance, a.Nonce, a.Root, a.CodeHash)
		} else {
			if err := plainStateCollector.Collect(key, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (fw *FillWorker) FillStorage(plainStateCollector *etl.Collector) error {
	it := fw.as.IterateStorageHistory(fw.txNum)
	var compositeKey = make([]byte, length.Addr+length.Incarnation+length.Hash)
	binary.BigEndian.PutUint64(compositeKey[20:], state.FirstContractIncarnation)
	for it.HasNext() {
		key, val, err := it.Next()
		if err != nil {
			return err
		}
		copy(compositeKey[:20], key[:20])
		copy(compositeKey[20+8:], key[20:])
		if len(val) > 0 {
			if err := plainStateCollector.Collect(compositeKey, val); err != nil {
				return err
			}
			//fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
		} else {
			if err := plainStateCollector.Collect(compositeKey, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (fw *FillWorker) FillCode(codeCollector, plainContractCollector *etl.Collector) error {
	it := fw.as.IterateCodeHistory(fw.txNum)
	var compositeKey = make([]byte, length.Addr+length.Incarnation)
	binary.BigEndian.PutUint64(compositeKey[length.Addr:], state.FirstContractIncarnation)

	for it.HasNext() {
		key, val, err := it.Next()
		if err != nil {
			return err
		}
		copy(compositeKey, key)
		if len(val) > 0 {

			codeHash, err := libcommon.HashData(val)
			if err != nil {
				return err
			}
			if err = codeCollector.Collect(codeHash[:], val); err != nil {
				return err
			}
			if err = plainContractCollector.Collect(compositeKey, codeHash[:]); err != nil {
				return err
			}
			//fmt.Printf("Code [%x] => %d\n", compositeKey, len(val))
		} else {
			if err := plainContractCollector.Collect(compositeKey, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *ScanWorker) BitmapAccounts() error {
	it := sw.as.IterateAccountsTxs()
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return err
		}
		sw.bitmap.Add(v)
	}
	return nil
}

func (sw *ScanWorker) BitmapStorage() error {
	it := sw.as.IterateStorageTxs()
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return err
		}
		sw.bitmap.Add(v)
	}
	return nil
}

func (sw *ScanWorker) BitmapCode() error {
	it := sw.as.IterateCodeTxs()
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return err
		}
		sw.bitmap.Add(v)
	}
	return nil
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (sw *ScanWorker) Bitmap() *roaring64.Bitmap { return &sw.bitmap }
