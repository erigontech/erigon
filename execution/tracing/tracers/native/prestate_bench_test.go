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

package native

import (
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func benchPrestateState(rng *rand.Rand, nAccounts, nContracts, codeSize, nSlots int) state {
	st := state{}
	for i := range nAccounts {
		var addr common.Address
		fillRandom(rng, addr[:])
		balance := uint256.NewInt(rng.Uint64())
		acc := &account{Balance: (*hexutil.U256)(balance), Nonce: rng.Uint64N(1000), Storage: map[common.Hash]common.Hash{}}
		if i < nContracts {
			code := make(hexutil.Bytes, codeSize)
			fillRandom(rng, code)
			var hash common.Hash
			fillRandom(rng, hash[:])
			acc.Code, acc.CodeHash = &code, &hash
			for range nSlots {
				var k, v common.Hash
				fillRandom(rng, k[:])
				fillRandom(rng, v[:])
				acc.Storage[k] = v
			}
		}
		st[accounts.InternAddress(addr)] = acc
	}
	return st
}

func fillRandom(rng *rand.Rand, b []byte) {
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
}

func BenchmarkPrestateResult(b *testing.B) {
	shapes := []struct {
		name                                 string
		accounts, contracts, codeSize, slots int
	}{
		{"small", 5, 1, 2 << 10, 10},
		{"medium", 50, 20, 10 << 10, 50},
		{"large", 500, 200, 24 << 10, 200},
	}
	for _, shape := range shapes {
		for _, diff := range []bool{false, true} {
			rng := rand.New(rand.NewPCG(1, 2))
			t := &prestateTracer{config: prestateTracerConfig{DiffMode: diff}}
			t.pre = benchPrestateState(rng, shape.accounts, shape.contracts, shape.codeSize, shape.slots)
			t.post = benchPrestateState(rng, shape.accounts/2, shape.contracts/2, shape.codeSize, shape.slots/4)
			name := fmt.Sprintf("%s/diff=%v", shape.name, diff)
			b.Run(name+"/reflect", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					s := jsonstream.Get(io.Discard)
					r, err := t.GetResult()
					if err != nil {
						b.Fatal(err)
					}
					s.WriteRawBytes(r)
					if err := s.Flush(); err != nil {
						b.Fatal(err)
					}
					jsonstream.Put(s)
				}
			})
			b.Run(name+"/fastjson", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					s := jsonstream.Get(io.Discard)
					if err := t.MarshalFastJSONTo(s); err != nil {
						b.Fatal(err)
					}
					if err := s.Flush(); err != nil {
						b.Fatal(err)
					}
					jsonstream.Put(s)
				}
			})
		}
	}
}
