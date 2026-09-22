// Copyright 2025 The Erigon Authors
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
	"encoding/json"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// RPCReceipts is eth_getBlockReceipts' answer. The RPC encoder only consults the top-level
// result for a fast marshaller, so a plain []*RPCReceipt would take the reflection path
// however the element is written.
type RPCReceipts []*RPCReceipt

func (rs RPCReceipts) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	if rs == nil {
		w.WriteNil()
		return nil
	}
	w.WriteArrayStart()
	for _, r := range rs {
		if err := r.MarshalFastJSONTo(w); err != nil {
			return err
		}
	}
	w.WriteArrayEnd()
	return nil
}

// MarshalFastJSONTo writes the receipt's fields in the order the struct declares them, so
// the bytes match reflection exactly.
func (r *RPCReceipt) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	if r == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()

	w.WriteObjectField("blockHash").WriteHex(r.BlockHash[:])
	jsonstream.Field(w, "blockNumber").WriteQuotedText(&r.BlockNumber)
	jsonstream.Field(w, "transactionHash").WriteHex(r.TransactionHash[:])
	jsonstream.Field(w, "transactionIndex").WriteQuotedText(&r.TransactionIndex)
	jsonstream.Field(w, "from")
	if r.From == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.From[:])
	}
	jsonstream.Field(w, "to")
	if r.To == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.To[:])
	}
	jsonstream.Field(w, "type").WriteQuotedText(&r.Type)
	jsonstream.Field(w, "gasUsed").WriteQuotedText(&r.GasUsed)
	jsonstream.Field(w, "cumulativeGasUsed").WriteQuotedText(&r.CumulativeGasUsed)
	jsonstream.Field(w, "contractAddress")
	if r.ContractAddress == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.ContractAddress[:])
	}
	jsonstream.Field(w, "logs")
	if err := writeLogs(w, r.Logs); err != nil {
		return err
	}
	jsonstream.Field(w, "logsBloom")
	if r.LogsBloom == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.LogsBloom[:])
	}

	if r.EffectiveGasPrice != nil {
		jsonstream.Field(w, "effectiveGasPrice").WriteQuotedText(r.EffectiveGasPrice)
	}
	if r.Status != nil {
		jsonstream.Field(w, "status").WriteQuotedText(r.Status)
	}
	if len(r.Root) > 0 {
		jsonstream.Field(w, "root").WriteHex(r.Root)
	}
	if r.BlobGasPrice != nil {
		jsonstream.Field(w, "blobGasPrice").WriteQuotedText(r.BlobGasPrice)
	}
	if r.BlobGasUsed != nil {
		jsonstream.Field(w, "blobGasUsed").WriteQuotedText(r.BlobGasUsed)
	}

	w.WriteObjectEnd()
	return nil
}

// writeLogs streams the shapes this package builds. Any other shape goes through
// encoding/json, which can only fail on a value no constructor here produces.
func writeLogs(w *jsonstream.StackStream, logs any) error {
	switch v := logs.(type) {
	case types.Logs:
		jsonstream.ArrayValue(w, v, writeLog)
	case []*types.Log:
		jsonstream.ArrayValue(w, v, writeLog)
	case []*types.RPCLog:
		jsonstream.ArrayValue(w, v, writeRPCLogElem)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		w.WriteRawBytes(b)
	}
	return nil
}

func writeRPCLogElem(w *jsonstream.StackStream, l **types.RPCLog) { _ = (*l).MarshalFastJSONTo(w) }

// writeLog writes one log in the order types.Log declares its fields.
func writeLog(w *jsonstream.StackStream, lp **types.Log) {
	l := *lp
	if l == nil {
		w.WriteNil()
		return
	}
	w.WriteObjectStart()
	w.WriteObjectField("address").WriteHex(l.Address[:])
	jsonstream.HexesField(w, "topics", l.Topics)
	jsonstream.Field(w, "data").WriteHex(l.Data)
	jsonstream.Field(w, "blockNumber").WriteQuotedText(&l.BlockNumber)
	jsonstream.Field(w, "transactionHash").WriteHex(l.TxHash[:])
	jsonstream.Field(w, "transactionIndex").WriteQuotedText(&l.TxIndex)
	jsonstream.Field(w, "blockHash").WriteHex(l.BlockHash[:])
	jsonstream.Field(w, "logIndex").WriteQuotedText(&l.Index)
	jsonstream.Field(w, "removed").WriteBool(l.Removed)
	w.WriteObjectEnd()
}
