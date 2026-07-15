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
	"errors"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/execution/rlp"
)

// storedTxnRLP returns the encoding rawdb.WriteTransactions persists, i.e. what
// the EthTx table and the transactions snapshot hold for one txn.
func storedTxnRLP(t testing.TB, txn Transaction) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := txn.EncodeRLP(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestTxnHashFromRLP pins TxnHashFromRLP against the decode-then-Hash path it
// replaces, across every transaction type.
func TestTxnHashFromRLP(t *testing.T) {
	tr := NewTRand()
	var buf bytes.Buffer
	for range RUNS {
		txn := tr.RandTransaction(-1)
		buf.Reset()
		if err := txn.EncodeRLP(&buf); err != nil {
			if txn.Type() >= BlobTxType && errors.Is(err, ErrNilToFieldTx) {
				continue
			}
			t.Fatalf("EncodeRLP: %v", err)
		}
		stored := buf.Bytes()

		decoded, err := DecodeTransaction(stored)
		if err != nil {
			t.Fatalf("DecodeTransaction: %v", err)
		}
		want := decoded.Hash()

		got, err := TxnHashFromRLP(stored)
		if err != nil {
			t.Fatalf("TxnHashFromRLP (type %d): %v", txn.Type(), err)
		}
		if got != want {
			t.Fatalf("type %d: hash mismatch: got %x, want %x", txn.Type(), got, want)
		}
	}
}

func TestTxnHashFromRLPErrors(t *testing.T) {
	tr := &TRand{rnd: rand.New(rand.NewSource(1))}
	legacy := storedTxnRLP(t, tr.RandTransaction(LegacyTxType))

	tests := []struct {
		name    string
		input   []byte
		wantErr error // nil means any error will do
	}{
		{"empty", nil, errEmptyTxnRLP},
		{"bare byte", []byte{0x01}, errShortTxnRLP},
		{"truncated string", []byte{0x84, 0x01}, nil},
		{"empty string", []byte{0x80}, errShortTxnRLP},
		{"trailing bytes", append(bytes.Clone(legacy), 0xFF), errTrailingBytes},
		// A 1-byte string holding a byte < 0x80 is non-canonical RLP, so rlp.Split
		// turns it down before the envelope guards below are reached.
		{"non-canonical envelope", []byte{0x81, 0x02}, rlp.ErrCanonSize},
		// Inputs that are well-formed RLP but cannot be a transaction. The decode
		// this replaces rejected them, so hashing them would index corrupt data.
		{"empty list", []byte{0xC0}, errEmptyTxnRLP},
		{"type byte only", []byte{0x81, 0x80}, errShortTxnRLP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := TxnHashFromRLP(tt.input)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestTxnHashFromRLPRejectsWhatDecodeRejects pins the guards above to the decode
// path they stand in for: TxnHashFromRLP must not accept an input DecodeTransaction
// turns down.
func TestTxnHashFromRLPRejectsWhatDecodeRejects(t *testing.T) {
	for _, input := range [][]byte{{0xC0}, {0x81, 0x80}, {0x81, 0x02}, {0x80}, {0x01}} {
		if _, err := DecodeTransaction(input); err == nil {
			t.Fatalf("DecodeTransaction(%x) unexpectedly succeeded; guard premise is wrong", input)
		}
		if _, err := TxnHashFromRLP(input); err == nil {
			t.Fatalf("TxnHashFromRLP(%x) accepted an input DecodeTransaction rejects", input)
		}
	}
}

func benchStoredTxn(b *testing.B, txType int) []byte {
	b.Helper()
	tr := &TRand{rnd: rand.New(rand.NewSource(1))}
	for range 100 {
		if txn := tr.RandTransaction(txType); txn.GetTo() != nil {
			return storedTxnRLP(b, txn)
		}
	}
	b.Fatalf("no transaction of type %d with a non-nil To", txType)
	return nil
}

var hashBenchTxTypes = []struct {
	name   string
	txType int
}{
	{"Legacy", LegacyTxType},
	{"AccessList", AccessListTxType},
	{"DynamicFee", DynamicFeeTxType},
	{"Blob", BlobTxType},
	{"SetCode", SetCodeTxType},
}

func BenchmarkTxnHashFromRLP(b *testing.B) {
	for _, tt := range hashBenchTxTypes {
		b.Run(tt.name, func(b *testing.B) {
			enc := benchStoredTxn(b, tt.txType)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := TxnHashFromRLP(enc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecodeTransactionThenHash is the path TxnHashFromRLP replaces.
func BenchmarkDecodeTransactionThenHash(b *testing.B) {
	for _, tt := range hashBenchTxTypes {
		b.Run(tt.name, func(b *testing.B) {
			enc := benchStoredTxn(b, tt.txType)
			b.ReportAllocs()
			for b.Loop() {
				txn, err := DecodeTransaction(enc)
				if err != nil {
					b.Fatal(err)
				}
				_ = txn.Hash()
			}
		})
	}
}
