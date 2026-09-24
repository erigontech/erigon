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

package types

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// RPCHeaderView is a header as a reply spells it: the header itself, borrowed rather than
// copied, plus the hash, which is computed from the header instead of stored in it. A reply
// that needs the hash names it here, so every field it writes has a declaration.
type RPCHeaderView struct {
	*Header `ethjson:"inline"`
	Hash    common.Hash `json:"hash" ethjson:"data"`
}

// NewRPCHeaderView hashes the header once for the reply that carries it.
func NewRPCHeaderView(h *Header) *RPCHeaderView {
	if h == nil {
		return nil
	}
	return &RPCHeaderView{Header: h, Hash: h.Hash()}
}

func (v *RPCHeaderView) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if v == nil || v.Header == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	v.Header.writeFastJSONFields(s)
	jsonstream.Data(s, "hash", v.Hash[:])
	s.WriteObjectEnd()
	return nil
}

// MarshalFastJSONTo writes the fields in the order Header's json tags declare them.
func (h *Header) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if h == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	h.writeFastJSONFields(s)
	s.WriteObjectEnd()
	return nil
}

func (h *Header) writeFastJSONFields(s *jsonstream.StackStream) {
	jsonstream.Data(s, "parentHash", h.ParentHash[:])
	jsonstream.Data(s, "sha3Uncles", h.UncleHash[:])
	jsonstream.Data(s, "miner", h.Coinbase[:])
	jsonstream.Data(s, "stateRoot", h.Root[:])
	jsonstream.Data(s, "transactionsRoot", h.TxHash[:])
	jsonstream.Data(s, "receiptsRoot", h.ReceiptHash[:])
	jsonstream.Data(s, "logsBloom", h.Bloom[:])
	jsonstream.Quantity256(s, "difficulty", &h.Difficulty)
	jsonstream.Quantity256(s, "number", &h.Number)
	jsonstream.Quantity(s, "gasLimit", h.GasLimit)
	jsonstream.Quantity(s, "gasUsed", h.GasUsed)
	jsonstream.Quantity(s, "timestamp", h.Time)
	jsonstream.Data(s, "extraData", h.Extra)
	jsonstream.Data(s, "mixHash", h.MixDigest[:])
	jsonstream.Data(s, "nonce", h.Nonce[:])
	if h.AuRaStep != 0 {
		jsonstream.Quantity(s, "auraStep", h.AuRaStep)
	}
	if len(h.AuRaSeal) != 0 {
		jsonstream.Data(s, "auraSeal", h.AuRaSeal)
	}
	jsonstream.Quantity256(s, "baseFeePerGas", h.BaseFee)
	writeHashField(s, "withdrawalsRoot", h.WithdrawalsHash)
	jsonstream.QuantityOrNull(s, "blobGasUsed", h.BlobGasUsed)
	jsonstream.QuantityOrNull(s, "excessBlobGas", h.ExcessBlobGas)
	writeHashField(s, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	writeHashField(s, "requestsHash", h.RequestsHash)
	writeHashField(s, "blockAccessListHash", h.BlockAccessListHash)
	if h.SlotNumber != nil {
		jsonstream.Quantity(s, "slotNumber", *h.SlotNumber)
	}
}

func writeHashField(s *jsonstream.StackStream, name string, h *common.Hash) {
	s.Field(name)
	if h == nil {
		s.WriteNil()
		return
	}
	s.WriteHex(h[:])
}
