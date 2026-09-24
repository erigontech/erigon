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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalFastJSONTo writes the fields in the order gen_header_json.go declares them, so the
// bytes match the generated MarshalJSON exactly.
func (h *Header) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if h == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "parentHash", &h.ParentHash)
	jsonstream.Hex(s, "sha3Uncles", &h.UncleHash)
	jsonstream.Hex(s, "miner", &h.Coinbase)
	jsonstream.Hex(s, "stateRoot", &h.Root)
	jsonstream.Hex(s, "transactionsRoot", &h.TxHash)
	jsonstream.Hex(s, "receiptsRoot", &h.ReceiptHash)
	jsonstream.Hex(s, "logsBloom", &h.Bloom)
	jsonstream.Hex(s, "difficulty", (*hexutil.U256)(&h.Difficulty))
	jsonstream.Hex(s, "number", (*hexutil.U256)(&h.Number))
	jsonstream.Hex(s, "gasLimit", (*hexutil.Uint64)(&h.GasLimit))
	jsonstream.Hex(s, "gasUsed", (*hexutil.Uint64)(&h.GasUsed))
	jsonstream.Hex(s, "timestamp", (*hexutil.Uint64)(&h.Time))
	jsonstream.Hex(s, "extraData", (*hexutil.Bytes)(&h.Extra))
	jsonstream.Hex(s, "mixHash", &h.MixDigest)
	jsonstream.Hex(s, "nonce", &h.Nonce)
	if h.AuRaStep != 0 {
		jsonstream.Hex(s, "auraStep", (*hexutil.Uint64)(&h.AuRaStep))
	}
	if len(h.AuRaSeal) != 0 {
		jsonstream.Hex(s, "auraSeal", (*hexutil.Bytes)(&h.AuRaSeal))
	}
	jsonstream.Hex(s, "baseFeePerGas", (*hexutil.U256)(h.BaseFee))
	jsonstream.Hex(s, "withdrawalsRoot", h.WithdrawalsHash)
	jsonstream.Hex(s, "blobGasUsed", (*hexutil.Uint64)(h.BlobGasUsed))
	jsonstream.Hex(s, "excessBlobGas", (*hexutil.Uint64)(h.ExcessBlobGas))
	jsonstream.Hex(s, "parentBeaconBlockRoot", h.ParentBeaconBlockRoot)
	jsonstream.Hex(s, "requestsHash", h.RequestsHash)
	jsonstream.Hex(s, "blockAccessListHash", h.BlockAccessListHash)
	jsonstream.HexOmitempty(s, "slotNumber", (*hexutil.Uint64)(h.SlotNumber))
	hash := h.Hash()
	jsonstream.Hex(s, "hash", &hash)
	s.WriteObjectEnd()
	return nil
}
