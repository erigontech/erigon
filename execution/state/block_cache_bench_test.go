// Copyright 2026 The Erigon Authors
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
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkBlockStateCacheAccountBlock(b *testing.B) {
	const txs = 200
	_, tx, domains := NewTestRwTx(b)
	domains.SetDisableInlineTouchKey(true)
	domains.SetInMemHistoryReads(true)

	coinbase := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	senders := make([]accounts.Address, txs)
	recipients := make([]accounts.Address, txs)
	for i := range txs {
		senders[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(1_000_000 + i))))
		recipients[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(2_000_000 + i))))
	}
	seedEnc := reprAcc{nonce: 7, balance: 1_000_000_000, codeHash: accounts.InternCodeHash(common.HexToHash("0xc0de")), incarnation: 1}.enc()
	for _, addr := range append(append([]accounts.Address{coinbase}, senders...), recipients...) {
		v := addr.Value()
		if err := domains.DomainPut(kv.AccountsDomain, tx, v[:], seedEnc, 1, nil); err != nil {
			b.Fatal(err)
		}
	}

	rules := &chain.Rules{}
	getter := domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	fullWrite := func(addr accounts.Address, balance, nonce uint64) []any {
		return []any{balanceWrite(addr, balance, 0), nonceWrite(addr, nonce, 0), incarnationWrite(addr, 1), codeHashWrite(addr, 0xcd)}
	}

	blockWrites := make([]*WriteSet, txs)
	for i := range txs {
		blockWrites[i] = newWriteSet(append(append(fullWrite(senders[i], 999_000_000, 8), fullWrite(recipients[i], 1_001_000_000, 7)...), fullWrite(coinbase, uint64(1_000_000_000+i), 7)...)...)
	}

	txNum := uint64(10)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		cache := NewBlockStateCache()
		reader := NewCurrentCachedReaderV3(getter, cache)
		for i := range txs {
			for _, addr := range []accounts.Address{senders[i], recipients[i], coinbase} {
				if _, err := reader.ReadAccountData(addr); err != nil {
					b.Fatal(err)
				}
			}
			txNum++
			if err := blockWrites[i].Apply(domains, tx, 1, txNum, nil, rules, cache, false); err != nil {
				b.Fatal(err)
			}
		}
		txNum++
		if err := cache.Flush(domains, tx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlockStateCacheStorageReads(b *testing.B) {
	const txs = 200
	_, tx, domains := NewTestRwTx(b)
	domains.SetDisableInlineTouchKey(true)
	domains.SetInMemHistoryReads(true)

	token := accounts.InternAddress(common.HexToAddress("0x70ce"))
	hot := accounts.InternKey(common.HexToHash("0x01"))
	from := make([]accounts.StorageKey, txs)
	to := make([]accounts.StorageKey, txs)
	for i := range txs {
		from[i] = accounts.InternKey(common.BigToHash(big.NewInt(int64(1_000_000 + i))))
		to[i] = accounts.InternKey(common.BigToHash(big.NewInt(int64(2_000_000 + i%50))))
	}
	seed := uint256.NewInt(1_000_000_000_000).Bytes()
	for _, k := range append(append([]accounts.StorageKey{hot}, from...), to...) {
		if err := domains.DomainPut(kv.StorageDomain, tx, []byte(storageCacheKey(token, k)), seed, 1, nil); err != nil {
			b.Fatal(err)
		}
	}

	rules := &chain.Rules{}
	getter := domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})
	blockWrites := make([]*WriteSet, txs)
	for i := range txs {
		blockWrites[i] = newWriteSet(storageWrite(token, from[i], 999_000_000_000), storageWrite(token, to[i], uint64(1_000_001_000_000+i)), storageWrite(token, hot, uint64(i+1)))
	}

	txNum := uint64(10)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		cache := NewBlockStateCache()
		worker := NewCachedReaderV3(getter, cache)
		current := NewCurrentCachedReaderV3(getter, cache)
		for i := range txs {
			for range 2 {
				for _, k := range []accounts.StorageKey{from[i], to[i], hot} {
					if _, _, err := worker.ReadAccountStorage(token, k); err != nil {
						b.Fatal(err)
					}
				}
			}
			txNum++
			if err := blockWrites[i].Apply(domains, tx, 1, txNum, nil, rules, cache, false); err != nil {
				b.Fatal(err)
			}
			if _, _, err := current.ReadAccountStorage(token, to[i]); err != nil {
				b.Fatal(err)
			}
			if _, _, err := NewHistoryReaderV3WithBlockCache(tx, domains, cache, txNum).ReadAccountStorage(token, hot); err != nil {
				b.Fatal(err)
			}
		}
		txNum++
		if err := cache.Flush(domains, tx); err != nil {
			b.Fatal(err)
		}
	}
}
