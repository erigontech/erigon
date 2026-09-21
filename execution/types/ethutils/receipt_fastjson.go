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

// MarshalFastJSONTo writes the receipt's fields in the order the struct declares them, so
// the bytes match reflection exactly.
func (r *RPCReceipt) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	if r == nil {
		w.WriteNil()
		return nil
	}
	// The contract has no way to retract a result that has already started, so a
	// shape the writer cannot encode has to fail before the first byte.
	if err := validateLogs(r.Logs); err != nil {
		return err
	}
	w.WriteObjectStart()

	w.WriteObjectField("blockHash")
	w.WriteHex(r.BlockHash[:])
	jsonstream.Field(w, "blockNumber").WriteQuotedText(&r.BlockNumber)
	jsonstream.Field(w, "transactionHash").WriteHex(r.TransactionHash[:])
	jsonstream.Field(w, "transactionIndex").WriteQuotedText(&r.TransactionIndex)
	jsonstream.Field(w, "from").WriteHex(r.From[:])
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
	writeLogs(w, r.Logs)
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

func validateLogs(logs any) error {
	switch v := logs.(type) {
	case nil, types.Logs, []*types.Log, []*types.RPCLog:
		return nil
	case []map[string]any:
		for _, entry := range v {
			if err := validateSubscribeLog(entry); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("ethutils: receipt logs of unexpected type %T", logs)
}

func validateSubscribeLog(entry map[string]any) error {
	known := 0
	for _, k := range subscribeLogKeys {
		val, ok := entry[k]
		if !ok {
			continue
		}
		known++
		switch val.(type) {
		case common.Address, common.Hash, hexutil.Bytes, []common.Hash:
		default:
			return fmt.Errorf("ethutils: subscribe log field %q of unexpected type %T", k, val)
		}
	}
	if known != len(entry) {
		// A key outside the known set would be dropped silently, where the reflection
		// path this replaces would have written it.
		return fmt.Errorf("ethutils: subscribe log has %d keys, %d of them known", len(entry), known)
	}
	return nil
}

// writeLogs handles both shapes MarshalReceipt puts in Logs: []*types.Log, and
// []*types.RPCLog when the caller asked for blockTimestamp.
// validateLogs has already rejected any shape not handled here.
func writeLogs(w *jsonstream.StackStream, logs any) {
	switch v := logs.(type) {
	case types.Logs:
		writeLogList(w, v)
	case []*types.Log:
		writeLogList(w, v)
	case []*types.RPCLog:
		if v == nil {
			w.WriteNil()
			return
		}
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
	case []map[string]any:
		// MarshalSubscribeReceipt's shape, which eth_sendRawTransactionSync answers with.
		writeSubscribeLogs(w, v)
	default:
		w.WriteNil()
	}
}

// subscribeLogKeys is every key MarshalSubscribeReceipt puts in a log entry, in the order
// encoding/json emits them.
var subscribeLogKeys = []string{"address", "data", "topics", "transactionHash"}

// writeSubscribeLogs writes the map form MarshalSubscribeReceipt builds. encoding/json
// orders an object's keys, and address is only present when the proto log carried one.
func writeSubscribeLogs(w *jsonstream.StackStream, logs []map[string]any) {
	if logs == nil {
		w.WriteNil()
		return
	}
	w.WriteArrayStart()
	for i, entry := range logs {
		if i > 0 {
			w.WriteMore()
		}
		if entry == nil {
			w.WriteNil()
			continue
		}
		w.WriteObjectStart()
		first := true
		for _, k := range subscribeLogKeys {
			val, ok := entry[k]
			if !ok {
				continue
			}
			if first {
				w.WriteObjectField(k)
				first = false
			} else {
				jsonstream.Field(w, k)
			}
			switch t := val.(type) {
			case common.Address:
				w.WriteHex(t[:])
			case common.Hash:
				w.WriteHex(t[:])
			case hexutil.Bytes:
				w.WriteHex(t)
			case []common.Hash:
				if t == nil {
					w.WriteNil()
					break
				}
				w.WriteArrayStart()
				for j := range t {
					if j > 0 {
						w.WriteMore()
					}
					w.WriteHex(t[j][:])
				}
				w.WriteArrayEnd()
			}
		}
		w.WriteObjectEnd()
	}
	w.WriteArrayEnd()
}

func writeLogList(w *jsonstream.StackStream, logs []*types.Log) {
	if logs == nil {
		w.WriteNil()
		return
	}
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
func writeLog(w *jsonstream.StackStream, l *types.Log, blockTimestamp *hexutil.Uint64) {
	if l == nil {
		w.WriteNil()
		return
	}
	w.WriteObjectStart()
	w.WriteObjectField("address")
	w.WriteHex(l.Address[:])
	jsonstream.Field(w, "topics")
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
	jsonstream.Field(w, "data").WriteHex(l.Data)
	jsonstream.Field(w, "blockNumber").WriteQuotedText(&l.BlockNumber)
	jsonstream.Field(w, "transactionHash").WriteHex(l.TxHash[:])
	jsonstream.Field(w, "transactionIndex").WriteQuotedText(&l.TxIndex)
	jsonstream.Field(w, "blockHash").WriteHex(l.BlockHash[:])
	jsonstream.Field(w, "logIndex").WriteQuotedText(&l.Index)
	jsonstream.Field(w, "removed").WriteBool(l.Removed)
	if blockTimestamp != nil {
		jsonstream.Field(w, "blockTimestamp").WriteQuotedText(blockTimestamp)
	}
	w.WriteObjectEnd()
}
