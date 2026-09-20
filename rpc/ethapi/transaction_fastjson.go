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
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// MarshalFastJSONTo writes the transaction's fields in the order the struct declares them,
// so the bytes match reflection exactly.
func (t *RPCTransaction) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if t == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()
	w.WriteObjectField("blockHash")
	if t.BlockHash == nil {
		w.WriteNil()
	} else {
		w.WriteHex(t.BlockHash[:])
	}
	jsonw.Text(w, "blockNumber", t.BlockNumber)
	jsonw.Text(w, "blockTimestamp", t.BlockTimestamp)
	jsonw.Hex(w, "from", t.From[:])
	jsonw.Text(w, "gas", &t.Gas)
	jsonw.Text(w, "gasPrice", t.GasPrice)
	if t.MaxPriorityFeePerGas != nil {
		jsonw.Text(w, "maxPriorityFeePerGas", t.MaxPriorityFeePerGas)
	}
	if t.MaxFeePerGas != nil {
		jsonw.Text(w, "maxFeePerGas", t.MaxFeePerGas)
	}
	jsonw.Hex(w, "hash", t.Hash[:])
	// A nil Input is "0x", not null, so it bypasses jsonw.Hex.
	jsonw.Field(w, "input").WriteHex(t.Input)
	jsonw.Text(w, "nonce", &t.Nonce)
	jsonw.Hex(w, "to", addrOrNull(t.To))
	jsonw.Text(w, "transactionIndex", t.TransactionIndex)
	jsonw.Text(w, "value", t.Value)
	jsonw.Text(w, "type", &t.Type)
	if t.Accesses != nil {
		jsonw.Array(w, "accessList", (*[]types.AccessTuple)(t.Accesses), writeAccessTuple)
	}
	if t.ChainID != nil {
		jsonw.Text(w, "chainId", t.ChainID)
	}
	if t.MaxFeePerBlobGas != nil {
		jsonw.Text(w, "maxFeePerBlobGas", t.MaxFeePerBlobGas)
	}
	// A plain slice with omitempty: empty is omitted, not [].
	if len(t.BlobVersionedHashes) > 0 {
		jsonw.Array(w, "blobVersionedHashes", &t.BlobVersionedHashes, writeHashElem)
	}
	if t.Authorizations != nil {
		jsonw.Array(w, "authorizationList", t.Authorizations, writeAuthorization)
	}
	jsonw.Text(w, "v", t.V)
	if t.YParity != nil {
		jsonw.Text(w, "yParity", t.YParity)
	}
	jsonw.Text(w, "r", t.R)
	jsonw.Text(w, "s", t.S)
	w.WriteObjectEnd()
	return nil
}

func writeAccessTuple(w jsonw.JSONWriter, a *types.AccessTuple) {
	w.WriteObjectStart()
	w.WriteObjectField("address").WriteHex(a.Address[:])
	jsonw.Array(w, "storageKeys", &a.StorageKeys, writeHashElem)
	w.WriteObjectEnd()
}

func writeAuthorization(w jsonw.JSONWriter, a *types.JsonAuthorization) {
	w.WriteObjectStart()
	w.WriteObjectField("chainId").WriteQuotedText(&a.ChainID)
	jsonw.Hex(w, "address", a.Address[:])
	jsonw.Text(w, "nonce", &a.Nonce)
	jsonw.Text(w, "yParity", &a.YParity)
	jsonw.Text(w, "r", &a.R)
	jsonw.Text(w, "s", &a.S)
	w.WriteObjectEnd()
}
