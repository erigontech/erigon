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

// MarshalFastJSONTo writes json.Marshal's output without reflection: every value but the key is hex.
func (r *AccProofResult) MarshalFastJSONTo(w hexutil.JSONWriter) error {
	w.WriteRaw(`{"address":`)
	w.WriteHex(r.Address[:])
	w.WriteRaw(`,"accountProof":`)
	writeHexArray(w, r.AccountProof)
	w.WriteRaw(`,"balance":`)
	writeU256(w, r.Balance)
	w.WriteRaw(`,"codeHash":`)
	w.WriteHex(r.CodeHash[:])
	w.WriteRaw(`,"nonce":`)
	var buf [20]byte
	nonce, _ := r.Nonce.AppendText(append(buf[:0], '"'))
	w.WriteRawBytes(append(nonce, '"'))
	w.WriteRaw(`,"storageHash":`)
	w.WriteHex(r.StorageHash[:])
	w.WriteRaw(`,"storageProof":`)
	if r.StorageProof == nil {
		w.WriteRaw(`null}`)
		return nil
	}
	w.WriteRaw(`[`)
	for i := range r.StorageProof {
		sp := &r.StorageProof[i]
		if i > 0 {
			w.WriteRaw(`,`)
		}
		w.WriteRaw(`{"key":`)
		w.WriteString(sp.Key)
		w.WriteRaw(`,"value":`)
		writeU256(w, sp.Value)
		w.WriteRaw(`,"proof":`)
		writeHexArray(w, sp.Proof)
		w.WriteRaw(`}`)
	}
	w.WriteRaw(`]}`)
	return nil
}

func writeHexArray(w hexutil.JSONWriter, items []hexutil.Bytes) {
	if items == nil {
		w.WriteRaw(`null`)
		return
	}
	w.WriteRaw(`[`)
	for i, item := range items {
		if i > 0 {
			w.WriteRaw(`,`)
		}
		w.WriteHex(item)
	}
	w.WriteRaw(`]`)
}

func writeU256(w hexutil.JSONWriter, v *hexutil.U256) {
	if v == nil {
		w.WriteRaw(`null`)
		return
	}
	var buf [68]byte
	enc, _ := v.AppendText(append(buf[:0], '"'))
	w.WriteRawBytes(append(enc, '"'))
}
