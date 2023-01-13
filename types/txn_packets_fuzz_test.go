//go:build !nofuzz

package types

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/u256"
)

func FuzzPooledTransactions66(f *testing.F) {
	f.Add([]byte{})
	f.Add(hexutility.MustDecodeHex("f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"))
	f.Add(hexutility.MustDecodeHex("d78257f8d2f83535358202008025a00010702dfeccc57dd2d2d2d2d2d2322a463ad555a2018d2bad0203390a0a0a0a0a0a0a256d01f62b45b2e1c21c"))
	f.Add(hexutility.MustDecodeHex("e8bfffffffffffffffffffffffff71e866666666955ef90c91f9fa08f96ebfbfbf007d765059effe33"))
	f.Fuzz(func(t *testing.T, in []byte) {
		t.Parallel()
		ctx := NewTxParseContext(*u256.N1)
		slots := TxSlots{}
		reqId, _, err := ParsePooledTransactions66(in, 0, ctx, &slots, nil)
		if err != nil {
			t.Skip()
		}

		var rlpTxs [][]byte
		for i := range slots.Txs {
			rlpTxs = append(rlpTxs, slots.Txs[i].Rlp)
		}
		_ = EncodePooledTransactions66(rlpTxs, reqId, nil)
		if err != nil {
			t.Skip()
		}
	})
}

func FuzzGetPooledTransactions66(f *testing.F) {
	f.Add([]byte{})
	f.Add(hexutility.MustDecodeHex("e68306f854e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328"))
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
