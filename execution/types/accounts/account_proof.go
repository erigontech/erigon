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

package accounts

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// Result structs for GetProof
type AccProofResult struct {
	Address      common.Address    `json:"address"`
	AccountProof []hexutil.Bytes   `json:"accountProof"`
	Balance      *hexutil.U256     `json:"balance"`
	CodeHash     common.Hash       `json:"codeHash"`
	Nonce        hexutil.Uint64    `json:"nonce"`
	StorageHash  common.Hash       `json:"storageHash"`
	StorageProof []StorProofResult `json:"storageProof"`
}
type StorProofResult struct {
	Key   string          `json:"key"`
	Value *hexutil.U256   `json:"value"`
	Proof []hexutil.Bytes `json:"proof"`
}

func (r *AccProofResult) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	w.WriteObjectStart()
	w.WriteObjectField("address")
	w.WriteHex(r.Address[:])
	writeField(w, "accountProof")
	writeHexArray(w, r.AccountProof)
	writeField(w, "balance")
	writeU256(w, r.Balance)
	writeField(w, "codeHash")
	w.WriteHex(r.CodeHash[:])
	writeField(w, "nonce")
	w.WriteHexUint64(uint64(r.Nonce))
	writeField(w, "storageHash")
	w.WriteHex(r.StorageHash[:])
	writeField(w, "storageProof")
	if r.StorageProof == nil {
		w.WriteNil()
		w.WriteObjectEnd()
		return nil
	}
	w.WriteArrayStart()
	for i := range r.StorageProof {
		if i > 0 {
			w.WriteMore()
		}
		sp := &r.StorageProof[i]
		w.WriteObjectStart()
		w.WriteObjectField("key")
		w.WriteString(sp.Key)
		writeField(w, "value")
		writeU256(w, sp.Value)
		writeField(w, "proof")
		writeHexArray(w, sp.Proof)
		w.WriteObjectEnd()
	}
	w.WriteArrayEnd()
	w.WriteObjectEnd()
	return nil
}

func writeField(w jsonw.JSONWriter, name string) {
	w.WriteMore()
	w.WriteObjectField(name)
}

func writeHexArray(w jsonw.JSONWriter, items []hexutil.Bytes) {
	if items == nil {
		w.WriteNil()
		return
	}
	w.WriteArrayStart()
	for i, item := range items {
		if i > 0 {
			w.WriteMore()
		}
		w.WriteHex(item)
	}
	w.WriteArrayEnd()
}

func writeU256(w jsonw.JSONWriter, v *hexutil.U256) {
	if v == nil {
		w.WriteNil()
		return
	}
	w.WriteHexU256((*uint256.Int)(v))
}
