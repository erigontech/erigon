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
	"github.com/erigontech/erigon/rpc/jsonstream"
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

func (r *AccProofResult) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	s.WriteObjectStart()
	jsonstream.Hex(s, "address", &r.Address)
	jsonstream.Hexes(s, "accountProof", r.AccountProof)
	jsonstream.Hex(s, "balance", r.Balance)
	jsonstream.Hex(s, "codeHash", &r.CodeHash)
	jsonstream.Hex(s, "nonce", &r.Nonce)
	jsonstream.Hex(s, "storageHash", &r.StorageHash)
	s.Field("storageProof")
	jsonstream.ArrayValue(s, r.StorageProof, writeStorProofElem)
	s.WriteObjectEnd()
	return nil
}

func writeStorProofElem(s *jsonstream.StackStream, sp *StorProofResult) {
	s.WriteObjectStart()
	s.Field("key").WriteString(sp.Key)
	jsonstream.Hex(s, "value", sp.Value)
	jsonstream.Hexes(s, "proof", sp.Proof)
	s.WriteObjectEnd()
}
