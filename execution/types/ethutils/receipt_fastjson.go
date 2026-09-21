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
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// RPCReceipts is eth_getBlockReceipts' answer. The RPC encoder only consults the top-level
// result for a fast marshaller, so a plain []*RPCReceipt would take the reflection path
// however the element is written.
type RPCReceipts []*RPCReceipt

func (rs RPCReceipts) MarshalFastJSONTo(w jsonw.JSONWriter) error {
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
func field(w jsonw.JSONWriter, name string) jsonw.JSONWriter {
	w.WriteMore()
	return w.WriteObjectField(name)
}

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
		if v == nil {
			w.WriteNil()
			return nil
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
		return writeSubscribeLogs(w, v)
	default:
		return fmt.Errorf("ethutils: receipt logs of unexpected type %T", logs)
	}
	return nil
}

// writeSubscribeLogs writes the map form MarshalSubscribeReceipt builds. encoding/json
// orders an object's keys, and address is only present when the proto log carried one.
func writeSubscribeLogs(w jsonw.JSONWriter, logs []map[string]any) error {
	if logs == nil {
		w.WriteNil()
		return nil
	}
	w.WriteArrayStart()
	for i, entry := range logs {
		if i > 0 {
			w.WriteMore()
		}
		w.WriteObjectStart()
		first := true
		for _, k := range []string{"address", "data", "topics", "transactionHash"} {
			val, ok := entry[k]
			if !ok {
				continue
			}
			if first {
				w.WriteObjectField(k)
				first = false
			} else {
				field(w, k)
			}
			switch t := val.(type) {
			case common.Address:
				w.WriteHex(t[:])
			case common.Hash:
				w.WriteHex(t[:])
			case hexutil.Bytes:
				w.WriteHex(t)
			case []common.Hash:
				w.WriteArrayStart()
				for j := range t {
					if j > 0 {
						w.WriteMore()
					}
					w.WriteHex(t[j][:])
				}
				w.WriteArrayEnd()
			default:
				return fmt.Errorf("ethutils: subscribe log field %q of unexpected type %T", k, val)
			}
		}
		w.WriteObjectEnd()
	}
	w.WriteArrayEnd()
	return nil
}

func writeLogList(w jsonw.JSONWriter, logs []*types.Log) {
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
func writeLog(w jsonw.JSONWriter, l *types.Log, blockTimestamp *hexutil.Uint64) {
	if l == nil {
		w.WriteNil()
		return
	}
	w.WriteObjectStart()
	w.WriteObjectField("address")
	w.WriteHex(l.Address[:])
	field(w, "topics")
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
