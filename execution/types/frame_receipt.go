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
	"encoding/json"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// Frame receipts encode per-frame outcomes. The aggregate status, logs and bloom
// are derived from them rather than included in the consensus payload.
type frameReceiptRLP struct {
	CumulativeGasUsed uint64
	Payer             *common.Address
	Frames            []FrameReceipt
}

func (r *Receipt) deriveFrameFields() {
	r.Logs = nil
	r.Status = ReceiptStatusSuccessful
	for _, frame := range r.FrameReceipts {
		r.Logs = append(r.Logs, frame.Logs...)
		if frame.Status != ReceiptStatusSuccessful {
			r.Status = ReceiptStatusFailed
		}
	}
	r.PostState = nil
	r.derivedBloom.Store(nil)
}

func (r *ReceiptForStorage) decodeFrameStorage(s *rlp.Stream) error {
	var err error
	if r.CumulativeGasUsed, err = s.Uint64(); err != nil {
		return err
	}
	if err := s.Decode(&r.Payer); err != nil {
		return err
	}
	if err := s.Decode(&r.FrameReceipts); err != nil {
		return err
	}
	if r.FirstLogIndexWithinBlock, err = s.Uint32(); err != nil {
		return err
	}
	if err := s.Decode(&r.TransactionIndex); err != nil {
		return err
	}
	if r.GasUsed, err = s.Uint64(); err != nil {
		return err
	}
	(*Receipt)(r).deriveFrameFields()
	return s.ListEnd()
}

type FrameReceipts []FrameReceipt

type frameReceiptJSON struct {
	Status  hexutil.Uint64    `json:"status"`
	GasUsed [2]hexutil.Uint64 `json:"gasUsed"`
	Logs    Logs              `json:"logs"`
}

func (r FrameReceipt) MarshalJSON() ([]byte, error) {
	logs := r.Logs
	if logs == nil {
		logs = Logs{}
	}
	return json.Marshal(frameReceiptJSON{hexutil.Uint64(r.Status), [2]hexutil.Uint64{hexutil.Uint64(r.GasUsed.Execution), hexutil.Uint64(r.GasUsed.State)}, logs})
}

func (r *FrameReceipt) UnmarshalJSON(input []byte) error {
	var dec frameReceiptJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	*r = FrameReceipt{uint64(dec.Status), mdgas.MdGas{Execution: uint64(dec.GasUsed[0]), State: uint64(dec.GasUsed[1])}, dec.Logs}
	return nil
}

func (receipts FrameReceipts) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, receipts, writeFrameReceiptElem)
	return nil
}

func writeFrameReceiptElem(s *jsonstream.Stream, receipt *FrameReceipt) {
	s.WriteObjectStart()
	ethjson.Quantity(s, "status", receipt.Status)
	s.Field("gasUsed").WriteArrayStart()
	s.WriteQuotedText(hexutil.Uint64(receipt.GasUsed.Execution))
	s.WriteQuotedText(hexutil.Uint64(receipt.GasUsed.State))
	s.WriteArrayEnd()
	s.Field("logs")
	logs := receipt.Logs
	if logs == nil {
		logs = Logs{}
	}
	_ = logs.MarshalFastJSONTo(s)
	s.WriteObjectEnd()
}
