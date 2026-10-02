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
	seed := accounts.NewAccount()
	seed.Nonce = 7
	seed.Balance.SetUint64(1_000_000_000)
	seed.CodeHash = accounts.InternCodeHash(common.HexToHash("0xc0de"))
	seed.Incarnation = 1
	seedEnc := accounts.SerialiseV3(&seed)
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
