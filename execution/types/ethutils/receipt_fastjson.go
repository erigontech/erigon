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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
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

	ethjson.Data(w, "blockHash", r.BlockHash[:])
	ethjson.Quantity(w, "blockNumber", r.BlockNumber)
	ethjson.Data(w, "transactionHash", r.TransactionHash[:])
	ethjson.Quantity(w, "transactionIndex", r.TransactionIndex)
	if r.From == nil {
		w.Field("from").WriteNil()
	} else {
		ethjson.Data(w, "from", r.From[:])
	}
	if r.To == nil {
		w.Field("to").WriteNil()
	} else {
		ethjson.Data(w, "to", r.To[:])
	}
	ethjson.Quantity(w, "type", r.Type)
	ethjson.Quantity(w, "gasUsed", r.GasUsed)
	ethjson.Quantity(w, "cumulativeGasUsed", r.CumulativeGasUsed)
	if r.ContractAddress == nil {
		w.Field("contractAddress").WriteNil()
	} else {
		ethjson.Data(w, "contractAddress", r.ContractAddress[:])
	}
	w.Field("logs")
	if err := writeLogs(w, r.Logs); err != nil {
		return err
	}
	if r.LogsBloom == nil {
		w.Field("logsBloom").WriteNil()
	} else {
		ethjson.Data(w, "logsBloom", r.LogsBloom[:])
	}

	if r.EffectiveGasPrice == nil {
		w.Field("effectiveGasPrice").WriteNil()
	} else {
		ethjson.Quantity256(w, "effectiveGasPrice", (*uint256.Int)(r.EffectiveGasPrice))
	}
	if r.Status != nil {
		ethjson.Quantity(w, "status", *r.Status)
	}
	if len(r.Root) > 0 {
		ethjson.Data(w, "root", r.Root)
	}
	if r.BlobGasPrice != nil {
		ethjson.Quantity256(w, "blobGasPrice", (*uint256.Int)(r.BlobGasPrice))
	}
	if r.BlobGasUsed != nil {
		ethjson.Quantity(w, "blobGasUsed", *r.BlobGasUsed)
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
	ethjson.Data(w, "address", l.Address[:])
	ethjson.DataList(w, "topics", l.Topics)
	ethjson.Data(w, "data", l.Data)
	ethjson.Quantity(w, "blockNumber", l.BlockNumber)
	ethjson.Data(w, "transactionHash", l.TxHash[:])
	ethjson.Quantity(w, "transactionIndex", l.TxIndex)
	ethjson.Data(w, "blockHash", l.BlockHash[:])
	ethjson.Quantity(w, "logIndex", l.Index)
	w.Field("removed").WriteBool(l.Removed)
	w.WriteObjectEnd()
}
