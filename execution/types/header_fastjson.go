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

// MarshalJSON encodes through MarshalFastJSONTo, so a header inside another value gets the
// same bytes as the RPC paths that stream it.
func (h *Header) MarshalJSON() ([]byte, error) {
	return jsonstream.Marshal(h)
}

// MarshalFastJSONTo writes the fields in the order headerJSONByDeclaration spells them.
func (h *Header) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if h == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	s.Field("parentHash").WriteHex(h.ParentHash[:])
	s.Field("sha3Uncles").WriteHex(h.UncleHash[:])
	s.Field("miner").WriteHex(h.Coinbase[:])
	s.Field("stateRoot").WriteHex(h.Root[:])
	s.Field("transactionsRoot").WriteHex(h.TxHash[:])
	s.Field("receiptsRoot").WriteHex(h.ReceiptHash[:])
	s.Field("logsBloom").WriteHex(h.Bloom[:])
	jsonstream.Quantity256(s, "difficulty", &h.Difficulty)
	jsonstream.Quantity256(s, "number", &h.Number)
	jsonstream.Quantity(s, "gasLimit", h.GasLimit)
	jsonstream.Quantity(s, "gasUsed", h.GasUsed)
	jsonstream.Quantity(s, "timestamp", h.Time)
	jsonstream.Data(s, "extraData", h.Extra)
	s.Field("mixHash").WriteHex(h.MixDigest[:])
	s.Field("nonce").WriteHex(h.Nonce[:])
	jsonstream.QuantityOmitZero(s, "auraStep", h.AuRaStep)
	jsonstream.DataOmitEmpty(s, "auraSeal", h.AuRaSeal)
	jsonstream.Quantity256(s, "baseFeePerGas", h.BaseFee)
	writeHashField(s, "withdrawalsRoot", h.WithdrawalsHash)
	jsonstream.QuantityOrNull(s, "blobGasUsed", h.BlobGasUsed)
	jsonstream.QuantityOrNull(s, "excessBlobGas", h.ExcessBlobGas)
	writeHashField(s, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	writeHashField(s, "requestsHash", h.RequestsHash)
	writeHashField(s, "blockAccessListHash", h.BlockAccessListHash)
	jsonstream.QuantityOmitNil(s, "slotNumber", h.SlotNumber)
	hash := h.Hash()
	s.Field("hash").WriteHex(hash[:])
	s.WriteObjectEnd()
	return nil
}

func writeHashField(s *jsonstream.StackStream, name string, h *common.Hash) {
	s.Field(name)
	if h == nil {
		s.WriteNil()
		return
	}
	s.WriteHex(h[:])
}
