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

	jsonstream.Hex(w, "blockHash", r.BlockHash)
	jsonstream.Hex(w, "blockNumber", r.BlockNumber)
	jsonstream.Hex(w, "transactionHash", r.TransactionHash)
	jsonstream.Hex(w, "transactionIndex", r.TransactionIndex)
	jsonstream.HexPtr(w, "from", r.From)
	jsonstream.HexPtr(w, "to", r.To)
	jsonstream.Hex(w, "type", r.Type)
	jsonstream.Hex(w, "gasUsed", r.GasUsed)
	jsonstream.Hex(w, "cumulativeGasUsed", r.CumulativeGasUsed)
	jsonstream.HexPtr(w, "contractAddress", r.ContractAddress)
	w.Field("logs")
	if err := writeLogs(w, r.Logs); err != nil {
		return err
	}
	jsonstream.HexPtr(w, "logsBloom", r.LogsBloom)
	jsonstream.HexPtrOmitempty(w, "effectiveGasPrice", r.EffectiveGasPrice)
	jsonstream.HexPtrOmitempty(w, "status", r.Status)
	jsonstream.HexOmitempty(w, "root", r.Root)
	jsonstream.HexPtrOmitempty(w, "blobGasPrice", r.BlobGasPrice)
	jsonstream.HexPtrOmitempty(w, "blobGasUsed", r.BlobGasUsed)

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
	jsonstream.Hex(w, "address", l.Address)
	jsonstream.Hexes(w, "topics", l.Topics)
	jsonstream.Hex(w, "data", l.Data)
	jsonstream.Hex(w, "blockNumber", l.BlockNumber)
	jsonstream.Hex(w, "transactionHash", l.TxHash)
	jsonstream.Hex(w, "transactionIndex", l.TxIndex)
	jsonstream.Hex(w, "blockHash", l.BlockHash)
	jsonstream.Hex(w, "logIndex", l.Index)
	w.Field("removed").WriteBool(l.Removed)
	w.WriteObjectEnd()
}
