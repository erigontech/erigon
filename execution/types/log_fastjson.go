// Copyright 2024 The Erigon Authors
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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

// quotedHexLen is the encoded size of n bytes as a quoted 0x-prefixed hex string.
func quotedHexLen(n int) int { return len(`"0x"`) + 2*n }

// maxQuotedUintLen bounds a quoted 0x-prefixed hex uint64.
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
	n := len(`{"address":,"topics":[],"data":,"blockNumber":,"transactionHash":,`) +
		len(`"transactionIndex":,"blockHash":,"logIndex":,"removed":false,"blockTimestamp":}`)
	n += quotedHexLen(length.Addr)
	n += len(l.Topics) * (quotedHexLen(length.Hash) + 1)
	n += quotedHexLen(len(l.Data))
	n += 2 * quotedHexLen(length.Hash) // transactionHash, blockHash
	n += 4 * maxQuotedUintLen          // blockNumber, transactionIndex, logIndex, blockTimestamp
	return n
}

// appendFastJSON writes the log in the field order encoding/json uses for the
// struct, so the output is byte-identical to reflection-based marshalling.
func (l *RPCLog) appendFastJSON(dst []byte) []byte {
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
	if l.Removed {
		dst = append(dst, `,"removed":true`...)
	} else {
		dst = append(dst, `,"removed":false`...)
	}
	dst = append(dst, `,"blockTimestamp":`...)
	dst = appendQuotedUint64(dst, l.BlockTimestamp)
	return append(dst, '}')
}

// MarshalFastJSON serializes the eth_getLogs result into one pre-sized buffer
// (direct hex encoding) instead of reflection. The count and each log's data
// length are known up front, so the size is exact enough to allocate once.
// Byte-identical to json.Marshal of the same value.
func (logs RPCLogs) MarshalFastJSON() ([]byte, error) {
	if logs == nil {
		return []byte("null"), nil
	}
	size := len("[]")
	for _, l := range logs {
		if l == nil {
			size += len("null,")
			continue
		}
		size += l.fastJSONLen() + 1
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for i, l := range logs {
		if i > 0 {
			out = append(out, ',')
		}
		if l == nil {
			out = append(out, "null"...)
			continue
		}
		out = l.appendFastJSON(out)
	}
	return append(out, ']'), nil
}
