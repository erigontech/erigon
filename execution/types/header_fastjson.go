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
	"github.com/erigontech/erigon/common/hexutil"
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
	jsonstream.Text(s, "difficulty", (*hexutil.U256)(&h.Difficulty))
	jsonstream.Text(s, "number", (*hexutil.U256)(&h.Number))
	jsonstream.Text(s, "gasLimit", (*hexutil.Uint64)(&h.GasLimit))
	jsonstream.Text(s, "gasUsed", (*hexutil.Uint64)(&h.GasUsed))
	jsonstream.Text(s, "timestamp", (*hexutil.Uint64)(&h.Time))
	s.Field("extraData").WriteHex(h.Extra)
	s.Field("mixHash").WriteHex(h.MixDigest[:])
	s.Field("nonce").WriteHex(h.Nonce[:])
	if h.AuRaStep != 0 {
		jsonstream.Text(s, "auraStep", (*hexutil.Uint64)(&h.AuRaStep))
	}
	if len(h.AuRaSeal) != 0 {
		s.Field("auraSeal").WriteHex(h.AuRaSeal)
	}
	jsonstream.Text(s, "baseFeePerGas", (*hexutil.U256)(h.BaseFee))
	writeHashField(s, "withdrawalsRoot", h.WithdrawalsHash)
	jsonstream.Text(s, "blobGasUsed", (*hexutil.Uint64)(h.BlobGasUsed))
	jsonstream.Text(s, "excessBlobGas", (*hexutil.Uint64)(h.ExcessBlobGas))
	writeHashField(s, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	writeHashField(s, "requestsHash", h.RequestsHash)
	writeHashField(s, "blockAccessListHash", h.BlockAccessListHash)
	if h.SlotNumber != nil {
		jsonstream.Text(s, "slotNumber", (*hexutil.Uint64)(h.SlotNumber))
	}
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
