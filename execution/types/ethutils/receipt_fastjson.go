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
	"fmt"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// MarshalFastJSONTo writes the receipt's fields in the order the struct declares them, so
// the bytes match reflection exactly.
func (r *RPCReceipt) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if r == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()

	w.WriteObjectField("blockHash")
	w.WriteHex(r.BlockHash[:])
	w.WriteMore()
	w.WriteObjectField("blockNumber")
	w.WriteQuotedText(&r.BlockNumber)
	w.WriteMore()
	w.WriteObjectField("transactionHash")
	w.WriteHex(r.TransactionHash[:])
	w.WriteMore()
	w.WriteObjectField("transactionIndex")
	w.WriteQuotedText(&r.TransactionIndex)
	w.WriteMore()
	w.WriteObjectField("from")
	w.WriteHex(r.From[:])
	w.WriteMore()
	w.WriteObjectField("to")
	if r.To == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.To[:])
	}
	w.WriteMore()
	w.WriteObjectField("type")
	w.WriteQuotedText(&r.Type)
	w.WriteMore()
	w.WriteObjectField("gasUsed")
	w.WriteQuotedText(&r.GasUsed)
	w.WriteMore()
	w.WriteObjectField("cumulativeGasUsed")
	w.WriteQuotedText(&r.CumulativeGasUsed)
	w.WriteMore()
	w.WriteObjectField("contractAddress")
	if r.ContractAddress == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.ContractAddress[:])
	}
	w.WriteMore()
	w.WriteObjectField("logs")
	if err := writeLogs(w, r.Logs); err != nil {
		return err
	}
	w.WriteMore()
	w.WriteObjectField("logsBloom")
	if r.LogsBloom == nil {
		w.WriteNil()
	} else {
		w.WriteHex(r.LogsBloom[:])
	}

	if r.EffectiveGasPrice != nil {
		w.WriteMore()
		w.WriteObjectField("effectiveGasPrice")
		w.WriteQuotedText(r.EffectiveGasPrice)
	}
	if r.Status != nil {
		w.WriteMore()
		w.WriteObjectField("status")
		w.WriteQuotedText(r.Status)
	}
	if len(r.Root) > 0 {
		w.WriteMore()
		w.WriteObjectField("root")
		w.WriteHex(r.Root)
	}
	if r.BlobGasPrice != nil {
		w.WriteMore()
		w.WriteObjectField("blobGasPrice")
		w.WriteQuotedText(r.BlobGasPrice)
	}
	if r.BlobGasUsed != nil {
		w.WriteMore()
		w.WriteObjectField("blobGasUsed")
		w.WriteQuotedText(r.BlobGasUsed)
	}

	w.WriteObjectEnd()
	return nil
}

// writeLogs handles both shapes MarshalReceipt puts in Logs: []*types.Log, and
// []*types.RPCLog when the caller asked for blockTimestamp.
func writeLogs(w jsonw.JSONWriter, logs any) error {
	switch v := logs.(type) {
	case nil:
		w.WriteNil()
	case types.Logs:
		writeLogList(w, v)
	case []*types.Log:
		writeLogList(w, v)
	case []*types.RPCLog:
		w.WriteArrayStart()
		for i, l := range v {
			if i > 0 {
				w.WriteMore()
			}
			if l == nil {
				w.WriteNil()
				continue
			}
			writeLog(w, &l.Log, &l.BlockTimestamp)
		}
		w.WriteArrayEnd()
	default:
		return fmt.Errorf("ethutils: receipt logs of unexpected type %T", logs)
	}
	return nil
}

func writeLogList(w jsonw.JSONWriter, logs []*types.Log) {
	w.WriteArrayStart()
	for i := range logs {
		if i > 0 {
			w.WriteMore()
		}
		writeLog(w, logs[i], nil)
	}
	w.WriteArrayEnd()
}

// writeLog writes one log in the order types.Log declares its fields, with RPCLog's
// blockTimestamp appended when the caller has one.
func writeLog(w jsonw.JSONWriter, l *types.Log, blockTimestamp *hexutil.Uint64) {
	if l == nil {
		w.WriteNil()
		return
	}
	w.WriteObjectStart()
	w.WriteObjectField("address")
	w.WriteHex(l.Address[:])
	w.WriteMore()
	w.WriteObjectField("topics")
	if l.Topics == nil {
		w.WriteNil()
	} else {
		w.WriteArrayStart()
		for i := range l.Topics {
			if i > 0 {
				w.WriteMore()
			}
			w.WriteHex(l.Topics[i][:])
		}
		w.WriteArrayEnd()
	}
	w.WriteMore()
	w.WriteObjectField("data")
	w.WriteHex(l.Data)
	w.WriteMore()
	w.WriteObjectField("blockNumber")
	w.WriteQuotedText(&l.BlockNumber)
	w.WriteMore()
	w.WriteObjectField("transactionHash")
	w.WriteHex(l.TxHash[:])
	w.WriteMore()
	w.WriteObjectField("transactionIndex")
	w.WriteQuotedText(&l.TxIndex)
	w.WriteMore()
	w.WriteObjectField("blockHash")
	w.WriteHex(l.BlockHash[:])
	w.WriteMore()
	w.WriteObjectField("logIndex")
	w.WriteQuotedText(&l.Index)
	w.WriteMore()
	w.WriteObjectField("removed")
	w.WriteBool(l.Removed)
	if blockTimestamp != nil {
		w.WriteMore()
		w.WriteObjectField("blockTimestamp")
		w.WriteQuotedText(blockTimestamp)
	}
	w.WriteObjectEnd()
}
