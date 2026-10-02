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
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// MarshalFastJSONTo writes the getPayload envelope, byte-identical to json.Marshal(r).
func (r *GetPayloadResponse) MarshalFastJSONTo(s *jsonstream.Stream) error {
	if r == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	s.Field("executionPayload")
	r.ExecutionPayload.writeTo(s)
	ethjson.Quantity256(s, "blockValue", (*uint256.Int)(r.BlockValue))
	s.Field("blobsBundle")
	if err := r.BlobsBundle.MarshalFastJSONTo(s); err != nil {
		return err
	}
	ethjson.Datas(s, "executionRequests", r.ExecutionRequests)
	s.Field("shouldOverrideBuilder").WriteBool(r.ShouldOverrideBuilder)
	s.WriteObjectEnd()
	return nil
}

// writeTo writes the payload in its struct's field order and encoding/json's forms.
func (p *ExecutionPayload) writeTo(s *jsonstream.Stream) {
	if p == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	ethjson.Data(s, "parentHash", p.ParentHash[:])
	ethjson.Data(s, "feeRecipient", p.FeeRecipient[:])
	ethjson.Data(s, "stateRoot", p.StateRoot[:])
	ethjson.Data(s, "receiptsRoot", p.ReceiptsRoot[:])
	ethjson.Data(s, "logsBloom", p.LogsBloom)
	ethjson.Data(s, "prevRandao", p.PrevRandao[:])
	ethjson.Quantity(s, "blockNumber", p.BlockNumber)
	ethjson.Quantity(s, "gasLimit", p.GasLimit)
	ethjson.Quantity(s, "gasUsed", p.GasUsed)
	ethjson.Quantity(s, "timestamp", p.Timestamp)
	ethjson.Data(s, "extraData", p.ExtraData)
	ethjson.Quantity256(s, "baseFeePerGas", (*uint256.Int)(p.BaseFeePerGas))
	ethjson.Data(s, "blockHash", p.BlockHash[:])
	ethjson.Datas(s, "transactions", p.Transactions)
	s.Field("withdrawals")
	_ = types.Withdrawals(p.Withdrawals).MarshalFastJSONTo(s)
	jsonstream.Text(s, "blobGasUsed", p.BlobGasUsed)
	jsonstream.Text(s, "excessBlobGas", p.ExcessBlobGas)
	if p.SlotNumber != nil {
		jsonstream.Text(s, "slotNumber", p.SlotNumber)
	}
	if p.BlockAccessList != nil {
		ethjson.Data(s, "blockAccessList", *p.BlockAccessList)
	}
	s.WriteObjectEnd()
}
