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

// MarshalFastJSONTo writes the getPayload envelope, byte-identical to json.Marshal(r).
func (r *GetPayloadResponse) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if r == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	s.Field("executionPayload")
	r.ExecutionPayload.writeTo(s)
	jsonstream.Text(s, "blockValue", r.BlockValue)
	s.Field("blobsBundle")
	if err := r.BlobsBundle.MarshalFastJSONTo(s); err != nil {
		return err
	}
	s.Field("executionRequests")
	jsonstream.ArrayValue(s, r.ExecutionRequests, writeHex)
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
	s.Field("parentHash").WriteHex(p.ParentHash[:])
	s.Field("feeRecipient").WriteHex(p.FeeRecipient[:])
	s.Field("stateRoot").WriteHex(p.StateRoot[:])
	s.Field("receiptsRoot").WriteHex(p.ReceiptsRoot[:])
	s.Field("logsBloom").WriteHex(p.LogsBloom)
	s.Field("prevRandao").WriteHex(p.PrevRandao[:])
	jsonstream.Text(s, "blockNumber", &p.BlockNumber)
	jsonstream.Text(s, "gasLimit", &p.GasLimit)
	jsonstream.Text(s, "gasUsed", &p.GasUsed)
	jsonstream.Text(s, "timestamp", &p.Timestamp)
	s.Field("extraData").WriteHex(p.ExtraData)
	jsonstream.Text(s, "baseFeePerGas", p.BaseFeePerGas)
	s.Field("blockHash").WriteHex(p.BlockHash[:])
	s.Field("transactions")
	jsonstream.ArrayValue(s, p.Transactions, writeHex)
	s.Field("withdrawals")
	_ = types.Withdrawals(p.Withdrawals).MarshalFastJSONTo(s)
	jsonstream.Text(s, "blobGasUsed", p.BlobGasUsed)
	jsonstream.Text(s, "excessBlobGas", p.ExcessBlobGas)
	if p.SlotNumber != nil {
		jsonstream.Text(s, "slotNumber", p.SlotNumber)
	}
	if p.BlockAccessList != nil {
		s.Field("blockAccessList").WriteHex(*p.BlockAccessList)
	}
	s.WriteObjectEnd()
}
