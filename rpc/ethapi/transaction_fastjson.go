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

package ethapi

import (
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalFastJSONTo writes the transaction's fields in the order the struct declares them,
// so the bytes match reflection exactly.
func (t *RPCTransaction) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if t == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	s.WriteObjectField("blockHash")
	if t.BlockHash == nil {
		s.WriteNil()
	} else {
		s.WriteHex(t.BlockHash[:])
	}
	jsonstream.Text(s, "blockNumber", t.BlockNumber)
	jsonstream.Text(s, "blockTimestamp", t.BlockTimestamp)
	jsonstream.Field(s, "from").WriteHex(t.From[:])
	jsonstream.Text(s, "gas", &t.Gas)
	jsonstream.Text(s, "gasPrice", t.GasPrice)
	if t.MaxPriorityFeePerGas != nil {
		jsonstream.Text(s, "maxPriorityFeePerGas", t.MaxPriorityFeePerGas)
	}
	if t.MaxFeePerGas != nil {
		jsonstream.Text(s, "maxFeePerGas", t.MaxFeePerGas)
	}
	jsonstream.Field(s, "hash").WriteHex(t.Hash[:])
	jsonstream.Field(s, "input").WriteHex(t.Input)
	jsonstream.Text(s, "nonce", &t.Nonce)
	jsonstream.Field(s, "to")
	if t.To == nil {
		s.WriteNil()
	} else {
		s.WriteHex(t.To[:])
	}
	jsonstream.Text(s, "transactionIndex", t.TransactionIndex)
	jsonstream.Text(s, "value", t.Value)
	jsonstream.Text(s, "type", &t.Type)
	if t.Accesses != nil {
		jsonstream.Field(s, "accessList")
		jsonstream.ArrayValue(s, *t.Accesses, writeAccessTuple)
	}
	if t.ChainID != nil {
		jsonstream.Text(s, "chainId", t.ChainID)
	}
	if t.MaxFeePerBlobGas != nil {
		jsonstream.Text(s, "maxFeePerBlobGas", t.MaxFeePerBlobGas)
	}
	// A plain slice with omitempty: empty is omitted, not [].
	if len(t.BlobVersionedHashes) > 0 {
		jsonstream.HexesField(s, "blobVersionedHashes", t.BlobVersionedHashes)
	}
	if t.Authorizations != nil {
		jsonstream.Field(s, "authorizationList")
		jsonstream.ArrayValue(s, *t.Authorizations, writeAuthorization)
	}
	jsonstream.Text(s, "v", t.V)
	if t.YParity != nil {
		jsonstream.Text(s, "yParity", t.YParity)
	}
	jsonstream.Text(s, "r", t.R)
	jsonstream.Text(s, "s", t.S)
	s.WriteObjectEnd()
	return nil
}

func writeAccessTuple(s *jsonstream.StackStream, a *types.AccessTuple) {
	s.WriteObjectStart()
	s.WriteObjectField("address").WriteHex(a.Address[:])
	jsonstream.HexesField(s, "storageKeys", a.StorageKeys)
	s.WriteObjectEnd()
}

func writeAuthorization(s *jsonstream.StackStream, a *types.JsonAuthorization) {
	s.WriteObjectStart()
	s.WriteObjectField("chainId").WriteQuotedText(&a.ChainID)
	jsonstream.Field(s, "address").WriteHex(a.Address[:])
	jsonstream.Text(s, "nonce", &a.Nonce)
	jsonstream.Text(s, "yParity", &a.YParity)
	jsonstream.Text(s, "r", &a.R)
	jsonstream.Text(s, "s", &a.S)
	s.WriteObjectEnd()
}
