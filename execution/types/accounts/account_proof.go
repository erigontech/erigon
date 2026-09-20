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
	w.WriteObjectField("address").WriteHex(r.Address[:])
	jsonw.Array(w, "accountProof", &r.AccountProof, writeHexElem)
	jsonw.Text(w, "balance", r.Balance)
	jsonw.Hex(w, "codeHash", r.CodeHash[:])
	jsonw.Text(w, "nonce", &r.Nonce)
	jsonw.Hex(w, "storageHash", r.StorageHash[:])
	jsonw.Array(w, "storageProof", &r.StorageProof, writeStorProofElem)
	w.WriteObjectEnd()
	return nil
}

func writeHexElem(w jsonw.JSONWriter, b *hexutil.Bytes) { w.WriteHex(*b) }

func writeStorProofElem(w jsonw.JSONWriter, sp *StorProofResult) {
	w.WriteObjectStart()
	w.WriteObjectField("key").WriteString(sp.Key)
	jsonw.Text(w, "value", sp.Value)
	jsonw.Array(w, "proof", &sp.Proof, writeHexElem)
	w.WriteObjectEnd()
}
