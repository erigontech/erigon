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

// A typed-tx envelope truncated to just its ChainID field must be rejected by
// the block body decoder, not silently dropped. Before the fix, the typed-tx
// DecodeRLP methods returned scalar field-read failures bare (`return err`), so
// a truncated tx yielded a bare rlp.EOL. decodeTxns/checkErrListEnd mistook it
// for a clean end of the transactions list, silently dropping the
// malformed transaction instead of rejecting the block (erigon would then
// accept a block other clients reject).
//
// The header built below declares the canonical EMPTY transactionsRoot,
// modeling the exact attacker scenario this bug enabled: a block that
// *looks* self-consistent (root matches "zero decoded txs") if and only if
// the malformed extra tx is swallowed rather than rejected.
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

// buildBlockWithTruncatedTypedTx returns the raw RLP bytes of a block whose
// header declares the empty transactionsRoot but whose body's transactions
// list contains exactly one truncated typed-tx element: an envelope
// containing only [chainID] and nothing else. E.g. for DynamicFeeTx
// (type 0x02) the tx-list element is 0x83 0x02 0xc1 0x01 -- a 3-byte RLP
// string "02 c1 01" (type byte 0x02 followed by the payload list 0xc1 0x01
// = [chainID=1]), itself wrapped in a 0xc4-prefixed one-element
// transactions list.
func buildBlockWithTruncatedTypedTx(t *testing.T, txType byte) []byte {
	t.Helper()
	header := &Header{
		UncleHash: empty.UncleHash,
		TxHash:    empty.TxsHash,
	}
	var headerBuf bytes.Buffer
	if err := header.EncodeRLP(&headerBuf); err != nil {
		t.Fatalf("encode header: %v", err)
	}

	txsList := []byte{0xc4, 0x83, txType, 0xc1, 0x01}
	unclesList := []byte{0xc0}
	withdrawalsList := []byte{0xc0}

	payloadSize := headerBuf.Len() + len(txsList) + len(unclesList) + len(withdrawalsList)
	var buf bytes.Buffer
	b := rlp.NewEncodingBuf()
	defer b.Release()
	if err := rlp.EncodeListPrefix(payloadSize, &buf, b[:]); err != nil {
		t.Fatalf("encode block list prefix: %v", err)
	}
	buf.Write(headerBuf.Bytes())
	buf.Write(txsList)
	buf.Write(unclesList)
	buf.Write(withdrawalsList)
	return buf.Bytes()
}
