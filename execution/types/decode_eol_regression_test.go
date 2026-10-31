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

package types

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
)

// The outer transaction list and each typed transaction payload have distinct
// RLP boundaries. Reaching the end of a payload while required fields are
// missing is an error, not the end of the outer list.
func TestDecodeEOLRegressionTruncatedTypedTx(t *testing.T) {
	tests := []struct {
		name   string
		txType byte
	}{
		{"DynamicFeeTx", DynamicFeeTxType},
		{"BlobTx", BlobTxType},
		{"SetCodeTx", SetCodeTxType},
		{"AccountAbstractionTx", AccountAbstractionTxType},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := buildBlockWithTruncatedTypedTx(t, tc.txType)
			s := rlp.NewBytesStream(data)
			defer rlp.PutStream(s)
			var b Block
			if err := b.DecodeRLP(s); err == nil {
				t.Fatalf("expected error decoding block with truncated type-%#x tx, got nil (len(txs)=%d)", tc.txType, len(b.transactions))
			}
		})
	}
}

// buildBlockWithTruncatedTypedTx builds a pre-Shanghai block whose only invalid
// component is a typed transaction payload ending after its chain ID. The
// canonical empty transaction and receipt roots make the block consistent with
// zero transactions, which is what a decoder that drops the payload would see.
func buildBlockWithTruncatedTypedTx(t *testing.T, txType byte) []byte {
	t.Helper()
	header := &Header{
		UncleHash:   empty.UncleHash,
		TxHash:      empty.TxsHash,
		ReceiptHash: empty.RootHash,
	}
	var headerBuf bytes.Buffer
	if err := header.EncodeRLP(&headerBuf); err != nil {
		t.Fatalf("encode header: %v", err)
	}

	txsList := []byte{0xc4, 0x83, txType, 0xc1, 0x01}
	unclesList := []byte{0xc0}

	payloadSize := headerBuf.Len() + len(txsList) + len(unclesList)
	var buf bytes.Buffer
	b := rlp.NewEncodingBuf()
	defer b.Release()
	if err := rlp.EncodeListPrefix(payloadSize, &buf, b[:]); err != nil {
		t.Fatalf("encode block list prefix: %v", err)
	}
	buf.Write(headerBuf.Bytes())
	buf.Write(txsList)
	buf.Write(unclesList)
	return buf.Bytes()
}
