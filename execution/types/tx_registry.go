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
	"fmt"
)

// TxTypeSpec describes an externally registered transaction type's decode
// facets. It is consulted only from the default arm of the built-in type
// switches, so an unregistered id keeps today's behavior unchanged.
type TxTypeSpec struct {
	New           func() Transaction
	UnmarshalJSON func([]byte) (Transaction, error)
}

var txTypeRegistry = map[byte]TxTypeSpec{}

// RegisterTxType registers spec for id. It panics if id collides with a
// built-in transaction type, lies outside the EIP-2718 typed-envelope range,
// or was already registered, and if spec.New is nil — all programming errors
// caught at init time.
func RegisterTxType(id byte, spec TxTypeSpec) {
	switch id {
	case LegacyTxType, AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType, AccountAbstractionTxType:
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
	if _, ok := txTypeRegistry[id]; ok {
		panic(fmt.Sprintf("types: RegisterTxType: %d already registered", id))
	}
	txTypeRegistry[id] = spec
}

func registeredTxType(id byte) (TxTypeSpec, bool) {
	spec, ok := txTypeRegistry[id]
	return spec, ok
}
