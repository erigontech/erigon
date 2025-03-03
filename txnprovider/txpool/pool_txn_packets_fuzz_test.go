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

//go:build !nofuzz

package txpool

import (
	"testing"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/u256"
)

func FuzzPooledTransactions66(f *testing.F) {
	f.Add([]byte{})
	f.Add(hexutil.MustDecodeHex("f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"))
	f.Add(hexutil.MustDecodeHex("d78257f8d2f83535358202008025a00010702dfeccc57dd2d2d2d2d2d2322a463ad555a2018d2bad0203390a0a0a0a0a0a0a256d01f62b45b2e1c21c"))
	f.Add(hexutil.MustDecodeHex("e8bfffffffffffffffffffffffff71e866666666955ef90c91f9fa08f96ebfbfbf007d765059effe33"))
	f.Fuzz(func(t *testing.T, in []byte) {
		t.Parallel()
		ctx := NewTxnParseContext(*u256.N1)
		slots := TxnSlots{}
		reqId, _, err := ParsePooledTransactions66(in, 0, ctx, &slots, nil)
		if err != nil {
			t.Skip()
		}

		var rlpTxns [][]byte
		for i := range slots.Txns {
			rlpTxns = append(rlpTxns, slots.Txns[i].Rlp)
		}
		_ = EncodePooledTransactions66(rlpTxns, reqId, nil)
		if err != nil {
			t.Skip()
		}
	})
}

func FuzzGetPooledTransactions66(f *testing.F) {
	f.Add([]byte{})
	f.Add(hexutil.MustDecodeHex("e68306f854e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328"))
	f.Fuzz(func(t *testing.T, in []byte) {
		t.Parallel()
		reqId, hashes, _, err := ParseGetPooledTransactions66(in, 0, nil)
		if err != nil {
			t.Skip()
		}

		_, err = EncodeGetPooledTransactions66(hashes, reqId, nil)
		if err != nil {
			t.Skip()
		}
	})
}
