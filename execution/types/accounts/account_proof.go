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
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
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

func (r *AccProofResult) MarshalFastJSONTo(s *jsonstream.Stream) error {
	s.WriteObjectStart()
	ethjson.Data(s, "address", r.Address[:])
	writeHexArray(s, "accountProof", r.AccountProof)
	ethjson.Quantity256(s, "balance", (*uint256.Int)(r.Balance))
	ethjson.Data(s, "codeHash", r.CodeHash[:])
	ethjson.Quantity(s, "nonce", r.Nonce)
	ethjson.Data(s, "storageHash", r.StorageHash[:])
	s.Field("storageProof")
	jsonstream.ArrayValue(s, r.StorageProof, writeStorProofElem)
	s.WriteObjectEnd()
	return nil
}

func writeHexArray(s *jsonstream.Stream, name string, nodes []hexutil.Bytes) {
	s.Field(name)
	if nodes == nil {
		s.WriteNil()
		return
	}
	jsonstream.WriteHexBytes(s, nodes)
}

func writeStorProofElem(s *jsonstream.Stream, sp *StorProofResult) {
	s.WriteObjectStart()
	s.Field("key").WriteString(sp.Key)
	ethjson.Quantity256(s, "value", (*uint256.Int)(sp.Value))
	writeHexArray(s, "proof", sp.Proof)
	s.WriteObjectEnd()
}
