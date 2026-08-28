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
	"fmt"
	"sync"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TxTypeSpec describes an externally registered transaction type. Registering
// an id admits it to the transaction decode and sender paths only; the receipt
// paths are separate and opt-in through StandardReceiptPayload.
type TxTypeSpec struct {
	New func() Transaction
	// UnmarshalJSON may be nil for types not submittable over JSON-RPC;
	// JSON decoding then rejects the type id as unknown.
	UnmarshalJSON func([]byte) (Transaction, error)
	// Sender resolves the transaction's sender with the caller's signer; it
	// must be self-contained and never call back into the signer's own
	// dispatch. Nil means sender recovery rejects the type.
	Sender func(txn Transaction, sg Signer) (accounts.Address, error)
	// StandardReceiptPayload declares that this type's receipts carry the
	// same payload as the built-in typed receipts. EIP-2718 leaves
	// ReceiptPayload opaque and type-specific, so a type that adds consensus
	// fields to it — as OP's deposit receipts do — must leave this false. Such
	// a type is then absent from the receipts root this package derives, and
	// the chain has to supply its own DerivableList.
	StandardReceiptPayload bool
}

var (
	txTypeRegistryMu sync.RWMutex
	txTypeRegistry   = map[byte]TxTypeSpec{}
)

// RegisterTxType registers spec for id. It panics if id collides with a
// built-in transaction type, lies outside the EIP-2718 typed-envelope range,
// or was already registered, and if spec.New is nil — all programming errors
// caught at init time.
func RegisterTxType(id byte, spec TxTypeSpec) {
	if builtinTxType(id) {
		panic(fmt.Sprintf("types: RegisterTxType: %d collides with a built-in transaction type", id))
	}
	if id >= 0x80 {
		// EIP-2718 reserves type bytes below 0x80; above it is the first byte
		// of a legacy RLP-encoded transaction.
		panic(fmt.Sprintf("types: RegisterTxType: %d outside the EIP-2718 type range", id))
	}
	if spec.New == nil {
		panic("types: RegisterTxType: spec.New is nil")
	}
	txTypeRegistryMu.Lock()
	defer txTypeRegistryMu.Unlock()
	if _, ok := txTypeRegistry[id]; ok {
		panic(fmt.Sprintf("types: RegisterTxType: %d already registered", id))
	}
	txTypeRegistry[id] = spec
}

func registeredTxType(id byte) (TxTypeSpec, bool) {
	txTypeRegistryMu.RLock()
	defer txTypeRegistryMu.RUnlock()
	spec, ok := txTypeRegistry[id]
	return spec, ok
}

// builtinTxType is the registration-collision list: the ids this package defines
// itself, which an external type may not claim.
func builtinTxType(id byte) bool {
	switch id {
	case LegacyTxType, AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType, AccountAbstractionTxType:
		return true
	}
	return false
}

// hasStandardReceiptPayload reports whether id's receipts are the typed receipt
// with the built-in payload. Its own list rather than builtinTxType: EIP-2718
// leaves ReceiptPayload type-specific, so a built-in that adds a consensus
// receipt field stays off this one. Legacy carries no type byte; a registered
// type has to opt in.
//
// AccountAbstractionTxType is off it deliberately — it has never had a receipt
// encoding, so giving it one moves the receipts root of an RIP-7560 block (#23569).
func hasStandardReceiptPayload(id byte) bool {
	switch id {
	case AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType:
		return true
	}
	spec, ok := registeredTxType(id)
	return ok && spec.StandardReceiptPayload
}

// storableReceiptType reports whether ReceiptForStorage can hold id's receipts
// without dropping a field. Wider than hasStandardReceiptPayload: the storage
// format keeps Type as a plain field, so every built-in round-trips whatever its
// consensus encoding. A registered type must still claim the standard payload —
// the format has no room for the extra fields the opt-out exists for.
func storableReceiptType(id byte) bool {
	return builtinTxType(id) || hasStandardReceiptPayload(id)
}
