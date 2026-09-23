// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types/testdata"
)

func BenchmarkParseTransaction(b *testing.B) {
	type benchCase struct {
		name             string
		chainID          uint256.Int
		payload          []byte
		wrappedWithBlobs bool
	}

	// Regular transactions from test vectors
	var cases []benchCase
	for _, ts := range allNetsTestCases {
		for i, tt := range ts.tests {
			cases = append(cases, benchCase{
				name:    fmt.Sprintf("regular/chain%d_tx%d", ts.chainID.Uint64(), i),
				chainID: ts.chainID,
				payload: hexutil.MustDecodeHex(tt.PayloadStr),
			})
		}
	}

	// Thin blob txn (no wrapper, just body with envelope)
	blobBodyRlpHex := "f9012705078502540be4008506fc23ac008357b58494811a752c8cd697e3cb27" +
		"279c330ed1ada745a8d7808204f7f872f85994de0b295669a9fd93d5f28d9ec85e40f4cb697b" +
		"aef842a00000000000000000000000000000000000000000000000000000000000000003a000" +
		"00000000000000000000000000000000000000000000000000000000000007d694bb9bc244d7" +
		"98123fde783fcc1c72d3bb8c189413c07bf842a0c6bdd1de713471bd6cfa62dd8b5a5b42969e" +
		"d09e26212d3377f3f8426d8ec210a08aaeccaf3873d07cef005aca28c39f8a9f8bdb1ec8d79f" +
		"fc25afc0a4fa2ab73601a036b241b061a36a32ab7fe86c7aa9eb592dd59018cd0443adc09035" +
		"90c16b02b0a05edcc541b4741c5cc6dd347c5ed9577ef293a62787b4510465fadbfe39ee4094"
	thinBlobPayload := hexutil.MustDecodeHex("b9012b") // envelope prefix
	thinBlobPayload = append(thinBlobPayload, BlobTxnType)
	thinBlobPayload = append(thinBlobPayload, hexutil.MustDecodeHex(blobBodyRlpHex)...)
	cases = append(cases,
		benchCase{
			name:    "blob/thin_envelope",
			chainID: *uint256.NewInt(5),
			payload: thinBlobPayload,
		},
		// Fat blob txn (BlobTxWrapper with 2 blobs, ~256KB)
		benchCase{
			name:             "blob/wrapper_2blobs",
			chainID:          *uint256.NewInt(5),
			payload:          buildBlobWrapperPayload(),
			wrappedWithBlobs: true,
		},
		// SetCode txn (EIP-7702, with 2 authorizations)
		benchCase{
			name:    "setcode/2auths",
			chainID: *uint256.NewInt(11155111),
			payload: hexutil.MustDecodeHex(testdata.ValidSetCodeTxn2),
		},
	)

	b.Run("WithSender", func(b *testing.B) {
		for _, bc := range cases {
			b.Run(bc.name, func(b *testing.B) {
				ctx := NewTxnParseContext(bc.chainID)
				slot := &TxnSlot{}
				sender := [20]byte{}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					*slot = TxnSlot{}
					_, _ = ctx.ParseTransaction(bc.payload, 0, slot, sender[:], false, bc.wrappedWithBlobs, nil)
				}
			})
		}
	})

	b.Run("WithoutSender", func(b *testing.B) {
		for _, bc := range cases {
			b.Run(bc.name, func(b *testing.B) {
				ctx := NewTxnParseContext(bc.chainID)
				ctx.WithSender(false)
				slot := &TxnSlot{}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					*slot = TxnSlot{}
					_, _ = ctx.ParseTransaction(bc.payload, 0, slot, nil, false, bc.wrappedWithBlobs, nil)
				}
			})
		}
	})
}
