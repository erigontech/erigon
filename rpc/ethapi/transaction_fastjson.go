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
	jsonstream.Hex(s, "blockHash", t.BlockHash)
	jsonstream.Hex(s, "blockNumber", t.BlockNumber)
	jsonstream.Hex(s, "blockTimestamp", t.BlockTimestamp)
	jsonstream.Hex(s, "from", &t.From)
	jsonstream.Hex(s, "gas", &t.Gas)
	jsonstream.Hex(s, "gasPrice", t.GasPrice)
	jsonstream.HexOmitempty(s, "maxPriorityFeePerGas", t.MaxPriorityFeePerGas)
	jsonstream.HexOmitempty(s, "maxFeePerGas", t.MaxFeePerGas)
	jsonstream.Hex(s, "hash", &t.Hash)
	jsonstream.Hex(s, "input", &t.Input)
	jsonstream.Hex(s, "nonce", &t.Nonce)
	jsonstream.Hex(s, "to", t.To)
	jsonstream.Hex(s, "transactionIndex", t.TransactionIndex)
	jsonstream.Hex(s, "value", t.Value)
	jsonstream.Hex(s, "type", &t.Type)
	if t.Accesses != nil {
		s.Field("accessList")
		jsonstream.ArrayValue(s, *t.Accesses, writeAccessTuple)
	}
	jsonstream.HexOmitempty(s, "chainId", t.ChainID)
	jsonstream.HexOmitempty(s, "maxFeePerBlobGas", t.MaxFeePerBlobGas)
	jsonstream.HexesOmitempty(s, "blobVersionedHashes", t.BlobVersionedHashes)
	if t.Authorizations != nil {
		s.Field("authorizationList")
		jsonstream.ArrayValue(s, *t.Authorizations, writeAuthorization)
	}
	jsonstream.Hex(s, "v", t.V)
	jsonstream.HexOmitempty(s, "yParity", t.YParity)
	jsonstream.Hex(s, "r", t.R)
	jsonstream.Hex(s, "s", t.S)
	s.WriteObjectEnd()
	return nil
}

func writeAccessTuple(s *jsonstream.StackStream, a *types.AccessTuple) {
	s.WriteObjectStart()
	jsonstream.Hex(s, "address", &a.Address)
	jsonstream.Hexes(s, "storageKeys", a.StorageKeys)
	s.WriteObjectEnd()
}

func writeAuthorization(s *jsonstream.StackStream, a *types.JsonAuthorization) {
	s.WriteObjectStart()
	jsonstream.Hex(s, "chainId", &a.ChainID)
	jsonstream.Hex(s, "address", &a.Address)
	jsonstream.Hex(s, "nonce", &a.Nonce)
	jsonstream.Hex(s, "yParity", &a.YParity)
	jsonstream.Hex(s, "r", &a.R)
	jsonstream.Hex(s, "s", &a.S)
	s.WriteObjectEnd()
}
