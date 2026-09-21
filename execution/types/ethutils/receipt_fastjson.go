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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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
	for i, r := range rs {
		if i > 0 {
			w.WriteMore()
		}
		if err := r.MarshalFastJSONTo(w); err != nil {
			return err
		}
	}
	w.WriteArrayEnd()
	return nil
}

// field writes the separator a following field needs, then its name, and returns the writer
// so the value chains onto it. An object's first field must not go through it.
func field(w *jsonstream.StackStream, name string) *jsonstream.StackStream {
	w.WriteMore()
	return w.WriteObjectField(name)
}

// MarshalFastJSONTo writes the receipt's fields in the order the struct declares them, so
// the bytes match reflection exactly.
func (r *RPCReceipt) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	if r == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()

	w.WriteObjectField("blockHash")
	w.WriteHex(r.BlockHash[:])
	field(w, "blockNumber").WriteQuotedText(&r.BlockNumber)
	field(w, "transactionHash").WriteHex(r.TransactionHash[:])
	field(w, "transactionIndex").WriteQuotedText(&r.TransactionIndex)
	field(w, "from").WriteHex(r.From[:])
	field(w, "to")
	if r.To == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.To[:])
	}
	field(w, "type").WriteQuotedText(&r.Type)
	field(w, "gasUsed").WriteQuotedText(&r.GasUsed)
	field(w, "cumulativeGasUsed").WriteQuotedText(&r.CumulativeGasUsed)
	field(w, "contractAddress")
	if r.ContractAddress == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.ContractAddress[:])
	}
	field(w, "logs")
	if err := writeLogs(w, r.Logs); err != nil {
		return err
	}
	field(w, "logsBloom")
	if r.LogsBloom == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.LogsBloom[:])
	}

	if r.EffectiveGasPrice != nil {
		field(w, "effectiveGasPrice").WriteQuotedText(r.EffectiveGasPrice)
	}
	if r.Status != nil {
		field(w, "status").WriteQuotedText(r.Status)
	}
	if len(r.Root) > 0 {
		field(w, "root").WriteHex(r.Root)
	}
	if r.BlobGasPrice != nil {
		field(w, "blobGasPrice").WriteQuotedText(r.BlobGasPrice)
	}
	if r.BlobGasUsed != nil {
		field(w, "blobGasUsed").WriteQuotedText(r.BlobGasUsed)
	}

	w.WriteObjectEnd()
	return nil
}

// writeLogs streams the shapes this package builds. Any other shape goes through
// encoding/json, which can only fail on a value no constructor here produces.
func writeLogs(w *jsonstream.StackStream, logs any) error {
	switch v := logs.(type) {
	case types.Logs:
		jsonstream.ArrayValue(w, v, writeLogElem)
	case []*types.Log:
		jsonstream.ArrayValue(w, v, writeLogElem)
	case []*types.RPCLog:
		jsonstream.ArrayValue(w, v, writeRPCLogElem)
	case []SubscribeLog:
		jsonstream.ArrayValue(w, v, writeSubscribeLog)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		w.WriteRawBytes(b)
	}
	return nil
}

func writeSubscribeLog(w *jsonstream.StackStream, l *SubscribeLog) {
	w.WriteObjectStart()
	if l.Address != nil {
		w.WriteObjectField("address").WriteHex(l.Address[:])
		w.WriteMore()
	}
	w.WriteObjectField("data").WriteHex(l.Data)
	field(w, "topics")
	jsonstream.ArrayValue(w, l.Topics, writeHash)
	field(w, "transactionHash").WriteHex(l.TransactionHash[:])
	w.WriteObjectEnd()
}

func writeLogElem(w *jsonstream.StackStream, l **types.Log) { writeLog(w, *l, nil) }

func writeRPCLogElem(w *jsonstream.StackStream, l **types.RPCLog) {
	if *l == nil {
		w.WriteNil()
		return
	}
	writeLog(w, &(*l).Log, &(*l).BlockTimestamp)
}

func writeHash(w *jsonstream.StackStream, h *common.Hash) { w.WriteHex(h[:]) }

// writeLog writes one log in the order types.Log declares its fields, with RPCLog's
// blockTimestamp appended when the caller has one.
func writeLog(w *jsonstream.StackStream, l *types.Log, blockTimestamp *hexutil.Uint64) {
	if l == nil {
		w.WriteNil()
		return
	}
	w.WriteObjectStart()
	w.WriteObjectField("address")
	w.WriteHex(l.Address[:])
	field(w, "topics")
	jsonstream.ArrayValue(w, l.Topics, writeHash)
	field(w, "data").WriteHex(l.Data)
	field(w, "blockNumber").WriteQuotedText(&l.BlockNumber)
	field(w, "transactionHash").WriteHex(l.TxHash[:])
	field(w, "transactionIndex").WriteQuotedText(&l.TxIndex)
	field(w, "blockHash").WriteHex(l.BlockHash[:])
	field(w, "logIndex").WriteQuotedText(&l.Index)
	field(w, "removed").WriteBool(l.Removed)
	if blockTimestamp != nil {
		field(w, "blockTimestamp").WriteQuotedText(blockTimestamp)
	}
	w.WriteObjectEnd()
}
