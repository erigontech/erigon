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
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

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
	ethjson.Data(s, "parentHash", h.ParentHash[:])
	ethjson.Data(s, "sha3Uncles", h.UncleHash[:])
	ethjson.Data(s, "miner", h.Coinbase[:])
	ethjson.Data(s, "stateRoot", h.Root[:])
	ethjson.Data(s, "transactionsRoot", h.TxHash[:])
	ethjson.Data(s, "receiptsRoot", h.ReceiptHash[:])
	ethjson.Data(s, "logsBloom", h.Bloom[:])
	ethjson.Quantity256(s, "difficulty", &h.Difficulty)
	ethjson.Quantity256(s, "number", &h.Number)
	ethjson.Quantity(s, "gasLimit", h.GasLimit)
	ethjson.Quantity(s, "gasUsed", h.GasUsed)
	ethjson.Quantity(s, "timestamp", h.Time)
	ethjson.Data(s, "extraData", h.Extra)
	ethjson.Data(s, "mixHash", h.MixDigest[:])
	ethjson.Data(s, "nonce", h.Nonce[:])
	ethjson.QuantityOmitEmpty(s, "auraStep", h.AuRaStep)
	ethjson.DataOmitEmpty(s, "auraSeal", h.AuRaSeal)
	ethjson.Quantity256(s, "baseFeePerGas", h.BaseFee)
	writeHashField(s, "withdrawalsRoot", h.WithdrawalsHash)
	ethjson.QuantityOrNull(s, "blobGasUsed", h.BlobGasUsed)
	ethjson.QuantityOrNull(s, "excessBlobGas", h.ExcessBlobGas)
	writeHashField(s, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	writeHashField(s, "requestsHash", h.RequestsHash)
	writeHashField(s, "blockAccessListHash", h.BlockAccessListHash)
	ethjson.QuantityPtrOmitEmpty(s, "slotNumber", h.SlotNumber)
	hash := h.Hash()
	ethjson.Data(s, "hash", hash[:])
}

func writeHashField(s *jsonstream.StackStream, name string, h *common.Hash) {
	s.Field(name)
	if h == nil {
		s.WriteNil()
		return
	}
	s.WriteHex(h[:])
}
