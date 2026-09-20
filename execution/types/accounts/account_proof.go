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
	{
		s.WriteObjectField("address")
		s.WriteHex(r.Address[:])
		writeField(s, "accountProof")
		writeHexArray(s, r.AccountProof)
		writeField(s, "balance")
		writeU256(s, r.Balance)
		writeField(s, "codeHash")
		s.WriteHex(r.CodeHash[:])
		writeField(s, "nonce")
		s.WriteQuotedText(&r.Nonce)
		writeField(s, "storageHash")
		s.WriteHex(r.StorageHash[:])
		writeField(s, "storageProof")
		if r.StorageProof == nil {
			s.WriteNil()
		} else {
			s.WriteArrayStart()
			for i := range r.StorageProof {
				if i > 0 {
					s.WriteMore()
				}
				sp := &r.StorageProof[i]
				s.WriteObjectStart()
				{
					s.WriteObjectField("key")
					s.WriteString(sp.Key)
					writeField(s, "value")
					writeU256(s, sp.Value)
					writeField(s, "proof")
					writeHexArray(s, sp.Proof)
				}
				s.WriteObjectEnd()
			}
			s.WriteArrayEnd()
		}
	}
	s.WriteObjectEnd()
	return nil
}

func writeField(s *jsonstream.StackStream, name string) {
	s.WriteMore()
	s.WriteObjectField(name)
}

func writeHexArray(s *jsonstream.StackStream, items []hexutil.Bytes) {
	if items == nil {
		s.WriteNil()
		return
	}
	s.WriteArrayStart()
	for i, item := range items {
		if i > 0 {
			s.WriteMore()
		}
		s.WriteHex(item)
	}
	s.WriteArrayEnd()
}

func writeU256(s *jsonstream.StackStream, v *hexutil.U256) {
	if v == nil {
		s.WriteNil()
		return
	}
	s.WriteQuotedText(v)
}
