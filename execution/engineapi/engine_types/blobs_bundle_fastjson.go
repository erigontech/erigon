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

package engine_types

import (
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalFastJSONTo streams the getPayload blobs bundle blob by blob, byte-identical to
// json.Marshal of the bundle.
func (b *BlobsBundle) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	writeBlobsBundle(s, b)
	return nil
}

func writeBlobsBundle(s *jsonstream.StackStream, b *BlobsBundle) {
	if b == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.Hexes(s, "commitments", b.Commitments)
	jsonstream.Hexes(s, "proofs", b.Proofs)
	jsonstream.Hexes(s, "blobs", b.Blobs)
	s.WriteObjectEnd()
}

// MarshalFastJSONTo writes the getPayload envelope, byte-identical to json.Marshal(r).
func (r *GetPayloadResponse) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if r == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	s.Field("executionPayload")
	r.ExecutionPayload.writeTo(s)
	jsonstream.Hex(s, "blockValue", r.BlockValue)
	s.Field("blobsBundle")
	writeBlobsBundle(s, r.BlobsBundle)
	jsonstream.Hexes(s, "executionRequests", r.ExecutionRequests)
	s.Field("shouldOverrideBuilder").WriteBool(r.ShouldOverrideBuilder)
	s.WriteObjectEnd()
	return nil
}

// writeTo writes the payload in its struct's field order and encoding/json's forms.
func (p *ExecutionPayload) writeTo(s *jsonstream.StackStream) {
	if p == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "parentHash", &p.ParentHash)
	jsonstream.Hex(s, "feeRecipient", &p.FeeRecipient)
	jsonstream.Hex(s, "stateRoot", &p.StateRoot)
	jsonstream.Hex(s, "receiptsRoot", &p.ReceiptsRoot)
	jsonstream.Hex(s, "logsBloom", &p.LogsBloom)
	jsonstream.Hex(s, "prevRandao", &p.PrevRandao)
	jsonstream.Hex(s, "blockNumber", &p.BlockNumber)
	jsonstream.Hex(s, "gasLimit", &p.GasLimit)
	jsonstream.Hex(s, "gasUsed", &p.GasUsed)
	jsonstream.Hex(s, "timestamp", &p.Timestamp)
	jsonstream.Hex(s, "extraData", &p.ExtraData)
	jsonstream.Hex(s, "baseFeePerGas", p.BaseFeePerGas)
	jsonstream.Hex(s, "blockHash", &p.BlockHash)
	jsonstream.Hexes(s, "transactions", p.Transactions)
	s.Field("withdrawals")
	jsonstream.ArrayValue(s, p.Withdrawals, writeWithdrawal)
	jsonstream.Hex(s, "blobGasUsed", p.BlobGasUsed)
	jsonstream.Hex(s, "excessBlobGas", p.ExcessBlobGas)
	jsonstream.HexOmitempty(s, "slotNumber", p.SlotNumber)
	jsonstream.HexOmitempty(s, "blockAccessList", p.BlockAccessList)
	s.WriteObjectEnd()
}

// writeWithdrawal never fails: Withdrawal.MarshalFastJSONTo reports no error.
func writeWithdrawal(s *jsonstream.StackStream, w **types.Withdrawal) { _ = (*w).MarshalFastJSONTo(s) }
