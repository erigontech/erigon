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

package types

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// The reference*Hash helpers reproduce the historical reflection-based ([]any)
// hashing so the encodePayload-based Hash() can be checked byte-for-byte.

func referenceLegacyHash(tx *LegacyTx) common.Hash {
	return RlpHash([]any{
		tx.Nonce,
		&tx.GasPrice,
		tx.GasLimit,
		tx.To,
		&tx.Value,
		tx.Data,
		tx.V, tx.R, tx.S,
	})
}

func referenceAccessListHash(tx *AccessListTx) common.Hash {
	return prefixedRlpHash(AccessListTxType, []any{
		&tx.ChainID,
		tx.Nonce,
		&tx.GasPrice,
		tx.GasLimit,
		tx.To,
		&tx.Value,
		tx.Data,
		tx.AccessList,
		tx.V, tx.R, tx.S,
	})
}

func referenceDynamicFeeHash(tx *DynamicFeeTransaction) common.Hash {
	return prefixedRlpHash(DynamicFeeTxType, []any{
		&tx.ChainID,
		tx.Nonce,
		&tx.TipCap,
		&tx.FeeCap,
		tx.GasLimit,
		tx.To,
		&tx.Value,
		tx.Data,
		tx.AccessList,
		tx.V, tx.R, tx.S,
	})
}

func referenceBlobHash(tx *BlobTx) common.Hash {
	return prefixedRlpHash(BlobTxType, []any{
		&tx.ChainID,
		tx.Nonce,
		&tx.TipCap,
		&tx.FeeCap,
		tx.GasLimit,
		tx.To,
		&tx.Value,
		tx.Data,
		tx.AccessList,
		&tx.MaxFeePerBlobGas,
		tx.BlobVersionedHashes,
		tx.V, tx.R, tx.S,
	})
}

func referenceSetCodeHash(tx *SetCodeTransaction) common.Hash {
	return prefixedRlpHash(SetCodeTxType, []any{
		&tx.ChainID,
		tx.Nonce,
		&tx.TipCap,
		&tx.FeeCap,
		tx.GasLimit,
		tx.To,
		&tx.Value,
		tx.Data,
		tx.AccessList,
		tx.Authorizations,
		tx.V, tx.R, tx.S,
	})
}

func TestTxHashMatchesReflectionReference(t *testing.T) {
	t.Parallel()
	tr := NewTRand()
	for range 300 {
		legacy := tr.RandTransaction(LegacyTxType).(*LegacyTx)
		assertHash(t, "legacy", legacy.Hash(), referenceLegacyHash(legacy))

		al := tr.RandTransaction(AccessListTxType).(*AccessListTx)
		assertHash(t, "accesslist", al.Hash(), referenceAccessListHash(al))

		dyn := tr.RandTransaction(DynamicFeeTxType).(*DynamicFeeTransaction)
		assertHash(t, "dynamicfee", dyn.Hash(), referenceDynamicFeeHash(dyn))

		blob := tr.RandTransaction(BlobTxType).(*BlobTx)
		assertHash(t, "blob", blob.Hash(), referenceBlobHash(blob))

		setcode := tr.RandTransaction(SetCodeTxType).(*SetCodeTransaction)
		assertHash(t, "setcode", setcode.Hash(), referenceSetCodeHash(setcode))
	}
}

func assertHash(t *testing.T, name string, got, want common.Hash) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: hash mismatch: got %x want %x", name, got, want)
	}
}

// TestAAHashMatchesMarshalBinary checks AccountAbstractionTransaction's hash against its
// canonical serialization. AA can't use the reflection oracle above: its SenderAddress
// (accounts.Address = unique.Handle) has unexported fields the reflection encoder skips,
// so the old []any hash dropped the sender. Hash() now uses encodePayload, matching the
// wire bytes from MarshalBinary.
func TestAAHashMatchesMarshalBinary(t *testing.T) {
	t.Parallel()
	tr := NewTRand()
	for range 100 {
		tx := tr.RandTransaction(AccountAbstractionTxType).(*AccountAbstractionTransaction)
		var buf bytes.Buffer
		if err := tx.MarshalBinary(&buf); err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		if got, want := tx.Hash(), crypto.HashData(buf.Bytes()); got != want {
			t.Fatalf("AA Hash %x != keccak(MarshalBinary) %x", got, want)
		}
	}
}

func BenchmarkTxHash(b *testing.B) {
	tr := NewTRand()
	for name, txType := range map[string]int{
		"legacy":     LegacyTxType,
		"accesslist": AccessListTxType,
		"dynamicfee": DynamicFeeTxType,
		"blob":       BlobTxType,
		"setcode":    SetCodeTxType,
		"aa":         AccountAbstractionTxType,
	} {
		tx := tr.RandTransaction(txType)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tx.Hash()
				resetTxHashCache(tx)
			}
		})
	}
}

func resetTxHashCache(tx Transaction) {
	switch t := tx.(type) {
	case *LegacyTx:
		t.hash.Store(nil)
	case *AccessListTx:
		t.hash.Store(nil)
	case *DynamicFeeTransaction:
		t.hash.Store(nil)
	case *BlobTx:
		t.hash.Store(nil)
	case *SetCodeTransaction:
		t.hash.Store(nil)
	case *AccountAbstractionTransaction:
		t.hash.Store(nil)
	}
}
