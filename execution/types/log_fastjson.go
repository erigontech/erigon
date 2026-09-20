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
	"strconv"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

func quotedHexLen(n int) int { return len(`"0x"`) + 2*n }

const maxQuotedUintLen = len(`"0x0123456789abcdef"`)

func appendQuotedHex(dst []byte, b []byte) []byte {
	dst = append(dst, '"')
	dst, _ = hexutil.Bytes(b).AppendText(dst)
	return append(dst, '"')
}

func appendQuotedUint64(dst []byte, v hexutil.Uint64) []byte {
	dst = append(dst, '"')
	dst, _ = v.AppendText(dst)
	return append(dst, '"')
}

// fastJSONLen is an upper bound on the encoded size, so the buffer is allocated
// once instead of doubling.
func (l *RPCLog) fastJSONLen() int {
	if l == nil {
		return len("null")
	}
	n := len(`{"address":,"topics":[],"data":,"blockNumber":,"transactionHash":,`) +
		len(`"transactionIndex":,"blockHash":,"logIndex":,"removed":false,"blockTimestamp":}`)
	n += quotedHexLen(length.Addr)
	if l.Topics == nil {
		n += len("null") - len("[]")
	}
	n += len(l.Topics) * (quotedHexLen(length.Hash) + 1)
	n += quotedHexLen(len(l.Data))
	n += 2 * quotedHexLen(length.Hash) // transactionHash, blockHash
	n += 4 * maxQuotedUintLen          // blockNumber, transactionIndex, logIndex, blockTimestamp
	return n
}

// appendFastJSON writes the log in the field order encoding/json uses for the
// struct, so the output is byte-identical to reflection-based marshalling.
func (l *RPCLog) AppendJSON(dst []byte) []byte {
	if l == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"address":`...)
	dst = appendQuotedHex(dst, l.Address[:])

	dst = append(dst, `,"topics":`...)
	if l.Topics == nil {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, '[')
		for i := range l.Topics {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendQuotedHex(dst, l.Topics[i][:])
		}
		dst = append(dst, ']')
	}

	dst = append(dst, `,"data":`...)
	dst = appendQuotedHex(dst, l.Data)
	dst = append(dst, `,"blockNumber":`...)
	dst = appendQuotedUint64(dst, l.BlockNumber)
	dst = append(dst, `,"transactionHash":`...)
	dst = appendQuotedHex(dst, l.TxHash[:])
	dst = append(dst, `,"transactionIndex":`...)
	dst = appendQuotedUint64(dst, hexutil.Uint64(l.TxIndex))
	dst = append(dst, `,"blockHash":`...)
	dst = appendQuotedHex(dst, l.BlockHash[:])
	dst = append(dst, `,"logIndex":`...)
	dst = appendQuotedUint64(dst, hexutil.Uint64(l.Index))
	dst = strconv.AppendBool(append(dst, `,"removed":`...), l.Removed)
	dst = append(dst, `,"blockTimestamp":`...)
	dst = appendQuotedUint64(dst, l.BlockTimestamp)
	return append(dst, '}')
}

// MarshalFastJSON is the single-log form of RPCLogs.MarshalFastJSON.
func (l *RPCLog) MarshalFastJSON() ([]byte, error) {
	return l.AppendJSON(make([]byte, 0, l.fastJSONLen())), nil
}

// MarshalFastJSON is byte-identical to json.Marshal, encoded into one buffer sized by fastJSONLen.
func (logs RPCLogs) MarshalFastJSON() ([]byte, error) {
	if logs == nil {
		return []byte("null"), nil
	}
	size := len("[]") + len(logs)
	for _, l := range logs {
		size += l.fastJSONLen()
	}
	out := append(make([]byte, 0, size), '[')
	for i, l := range logs {
		if i > 0 {
			out = append(out, ',')
		}
		out = l.AppendJSON(out)
	}
	return append(out, ']'), nil
}

// MarshalFastJSONTo writes the same bytes as appendFastJSON straight into the response
// stream, so a result never needs a buffer of its own.
func (l *RPCLog) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if l == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()
	w.WriteObjectField("address").WriteHex(l.Address[:])
	jsonw.Array(w, "topics", &l.Topics, writeTopic)
	// A nil Data is "0x", not null, so it bypasses jsonw.Hex.
	jsonw.Field(w, "data").WriteHex(l.Data)
	jsonw.Text(w, "blockNumber", &l.BlockNumber)
	jsonw.Hex(w, "transactionHash", l.TxHash[:])
	jsonw.Text(w, "transactionIndex", &l.TxIndex)
	jsonw.Hex(w, "blockHash", l.BlockHash[:])
	jsonw.Text(w, "logIndex", &l.Index)
	jsonw.Field(w, "removed").WriteBool(l.Removed)
	jsonw.Text(w, "blockTimestamp", &l.BlockTimestamp)
	w.WriteObjectEnd()
	return nil
}

func writeTopic(w jsonw.JSONWriter, h *common.Hash) { w.WriteHex(h[:]) }

// MarshalFastJSONTo writes the logs as a bare array. The receiver must stay a value: with a
// pointer method RPCLogs itself would not satisfy the fast-JSON interface.
func (logs RPCLogs) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	jsonw.ArrayValue(w, logs, writeLogElem)
	return nil
}

func writeLogElem(w jsonw.JSONWriter, l **RPCLog) { w.AppendJSON(*l) }
