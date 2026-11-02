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

package ethutils

import (
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// RPCFrameReceipts exposes scalar gas totals and both dimensions. The fixture codec
// in types.FrameReceipt uses a gas pair instead.
type RPCFrameReceipts []types.FrameReceipt

func (receipts RPCFrameReceipts) MarshalJSON() ([]byte, error) {
	return jsonstream.Marshal(receipts)
}

func (receipts RPCFrameReceipts) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, receipts, func(s *jsonstream.Stream, receipt *types.FrameReceipt) {
		s.WriteObjectStart()
		ethjson.Quantity(s, "status", receipt.Status)
		ethjson.Quantity(s, "gasUsed", receipt.GasUsed.Total())
		ethjson.Quantity(s, "executionGasUsed", receipt.GasUsed.Execution)
		ethjson.Quantity(s, "stateGasUsed", receipt.GasUsed.State)
		s.Field("logs")
		logs := types.Logs(receipt.Logs)
		if logs == nil {
			logs = types.Logs{}
		}
		_ = logs.MarshalFastJSONTo(s)
		s.WriteObjectEnd()
	})
	return nil
}
