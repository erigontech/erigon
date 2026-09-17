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

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

const maxQuotedUintLen = len(`"0x0123456789abcdef"`)

func appendQuotedUint64(dst []byte, v hexutil.Uint64) []byte {
	dst = append(dst, '"')
	dst, _ = v.AppendText(dst)
	return append(dst, '"')
}

// JSONLen is an upper bound on the encoded size, so the buffer is allocated
// once instead of doubling.
func (l *RPCLog) JSONLen() int {
	if l == nil {
		return len("null")
	}
	n := len(`{"address":,"topics":[],"data":,"blockNumber":,"transactionHash":,`) +
		len(`"transactionIndex":,"blockHash":,"logIndex":,"removed":false,"blockTimestamp":}`)
	n += hexutil.QuotedLen(length.Addr)
	if l.Topics == nil {
		n += len("null") - len("[]")
	}
	n += len(l.Topics) * (hexutil.QuotedLen(length.Hash) + 1)
	n += hexutil.QuotedLen(len(l.Data))
	n += 2 * hexutil.QuotedLen(length.Hash) // transactionHash, blockHash
	n += 4 * maxQuotedUintLen               // blockNumber, transactionIndex, logIndex, blockTimestamp
	return n
}

// AppendJSON writes the log in the field order encoding/json uses for the
// struct, so the output is byte-identical to reflection-based marshalling.
func (l *RPCLog) AppendJSON(dst []byte) []byte {
	if l == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, `{"address":`...)
	dst = hexutil.AppendQuoted(dst, l.Address[:])

	dst = append(dst, `,"topics":`...)
	if l.Topics == nil {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, '[')
		for i := range l.Topics {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = hexutil.AppendQuoted(dst, l.Topics[i][:])
		}
		dst = append(dst, ']')
	}

	dst = append(dst, `,"data":`...)
	dst = hexutil.AppendQuoted(dst, l.Data)
	dst = append(dst, `,"blockNumber":`...)
	dst = appendQuotedUint64(dst, l.BlockNumber)
	dst = append(dst, `,"transactionHash":`...)
	dst = hexutil.AppendQuoted(dst, l.TxHash[:])
	dst = append(dst, `,"transactionIndex":`...)
	dst = appendQuotedUint64(dst, hexutil.Uint64(l.TxIndex))
	dst = append(dst, `,"blockHash":`...)
	dst = hexutil.AppendQuoted(dst, l.BlockHash[:])
	dst = append(dst, `,"logIndex":`...)
	dst = appendQuotedUint64(dst, hexutil.Uint64(l.Index))
	dst = strconv.AppendBool(append(dst, `,"removed":`...), l.Removed)
	dst = append(dst, `,"blockTimestamp":`...)
	dst = appendQuotedUint64(dst, l.BlockTimestamp)
	return append(dst, '}')
}

func (l *RPCLog) MarshalFastJSON() ([]byte, error) {
	return l.AppendJSON(make([]byte, 0, l.JSONLen())), nil
}

func (l *RPCLog) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	w.WriteValue(l)
	return nil
}

func (logs RPCLogs) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if logs == nil {
		w.WriteNil()
		return nil
	}
	w.WriteArrayStart()
	for i, l := range logs {
		if i > 0 {
			w.WriteMore()
		}
		w.WriteValue(l)
	}
	w.WriteArrayEnd()
	return nil
}
