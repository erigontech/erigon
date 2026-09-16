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
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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

// AppendFastJSON appends json.Marshal's output without reflection and without the escape scan: all values are hex.
func (r *AccProofResult) AppendFastJSON(b []byte) ([]byte, error) {
	size := 256 + hexArraySize(r.AccountProof)
	for i := range r.StorageProof {
		if !isAlphanumeric(r.StorageProof[i].Key) {
			enc, err := json.Marshal(r)
			return append(b, enc...), err
		}
		size += 128 + len(r.StorageProof[i].Key) + hexArraySize(r.StorageProof[i].Proof)
	}
	b = slices.Grow(b, size)
	b = append(b, `{"address":"`...)
	b, _ = r.Address.AppendText(b)
	b = append(b, `","accountProof":`...)
	b = appendHexArray(b, r.AccountProof)
	b = append(b, `,"balance":`...)
	b = appendU256(b, r.Balance)
	b = append(b, `,"codeHash":"`...)
	b, _ = r.CodeHash.AppendText(b)
	b = append(b, `","nonce":"`...)
	b, _ = r.Nonce.AppendText(b)
	b = append(b, `","storageHash":"`...)
	b, _ = r.StorageHash.AppendText(b)
	b = append(b, `","storageProof":`...)
	if r.StorageProof == nil {
		return append(b, "null}"...), nil
	}
	b = append(b, '[')
	for i := range r.StorageProof {
		if i > 0 {
			b = append(b, ',')
		}
		sp := &r.StorageProof[i]
		b = append(b, `{"key":"`...)
		b = append(b, sp.Key...)
		b = append(b, `","value":`...)
		b = appendU256(b, sp.Value)
		b = append(b, `,"proof":`...)
		b = appendHexArray(b, sp.Proof)
		b = append(b, '}')
	}
	return append(b, "]}"...), nil
}

func isAlphanumeric(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; !('0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z') {
			return false
		}
	}
	return true
}

func hexArraySize(items []hexutil.Bytes) int {
	n := 2
	for _, item := range items {
		n += 2*len(item) + 5
	}
	return n
}

func appendHexArray(b []byte, items []hexutil.Bytes) []byte {
	if items == nil {
		return append(b, "null"...)
	}
	b = append(b, '[')
	for i, item := range items {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `"0x`...)
		b = hex.AppendEncode(b, item)
		b = append(b, '"')
	}
	return append(b, ']')
}

func appendU256(b []byte, v *hexutil.U256) []byte {
	if v == nil {
		return append(b, "null"...)
	}
	b = append(b, '"')
	b, _ = v.AppendText(b)
	return append(b, '"')
}
