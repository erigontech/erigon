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

	w.Field("blockHash").WriteHex(r.BlockHash[:])
	w.Field("blockNumber").WriteQuotedText(&r.BlockNumber)
	w.Field("transactionHash").WriteHex(r.TransactionHash[:])
	w.Field("transactionIndex").WriteQuotedText(&r.TransactionIndex)
	w.Field("from")
	if r.From == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.From[:])
	}
	w.Field("to")
	if r.To == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.To[:])
	}
	w.Field("type").WriteQuotedText(&r.Type)
	w.Field("gasUsed").WriteQuotedText(&r.GasUsed)
	w.Field("cumulativeGasUsed").WriteQuotedText(&r.CumulativeGasUsed)
	w.Field("contractAddress")
	if r.ContractAddress == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.ContractAddress[:])
	}
	w.Field("logs")
	if err := writeLogs(w, r.Logs); err != nil {
		return err
	}
	w.Field("logsBloom")
	if r.LogsBloom == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.LogsBloom[:])
	}

	if r.EffectiveGasPrice != nil {
		w.Field("effectiveGasPrice").WriteQuotedText(r.EffectiveGasPrice)
	}
	if r.Status != nil {
		w.Field("status").WriteQuotedText(r.Status)
	}
	if len(r.Root) > 0 {
		w.Field("root").WriteHex(r.Root)
	}
	if r.BlobGasPrice != nil {
		w.Field("blobGasPrice").WriteQuotedText(r.BlobGasPrice)
	}
	if r.BlobGasUsed != nil {
		w.Field("blobGasUsed").WriteQuotedText(r.BlobGasUsed)
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
	w.Field("address").WriteHex(l.Address[:])
	jsonstream.HexesField(w, "topics", l.Topics)
	w.Field("data").WriteHex(l.Data)
	w.Field("blockNumber").WriteQuotedText(&l.BlockNumber)
	w.Field("transactionHash").WriteHex(l.TxHash[:])
	w.Field("transactionIndex").WriteQuotedText(&l.TxIndex)
	w.Field("blockHash").WriteHex(l.BlockHash[:])
	w.Field("logIndex").WriteQuotedText(&l.Index)
	w.Field("removed").WriteBool(l.Removed)
	w.WriteObjectEnd()
}
