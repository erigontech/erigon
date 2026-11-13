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
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// MarshalFastJSONTo writes the logs as a bare array, each as Log.MarshalFastJSONTo does. The members
// that consecutive logs of one transaction or block share are encoded once and reused. The
// receiver must stay a value: with a pointer method Logs itself would not satisfy the fast-JSON
// interface.
func (logs Logs) MarshalFastJSONTo(s *jsonstream.Stream) error {
	if logs == nil {
		s.WriteNil()
		return nil
	}
	var shared sharedLogFields
	s.WriteArrayStart()
	for _, l := range logs {
		if l == nil {
			s.WriteNil()
			continue
		}
		s.WriteObjectStart()
		ethjson.Data(s, "address", l.Address[:])
		ethjson.DataList(s, "topics", l.Topics)
		ethjson.Data(s, "data", l.Data)
		s.RawFields(shared.position(l))
		ethjson.Quantity(s, "logIndex", l.Index)
		s.RawFields(shared.state(l))
		s.WriteObjectEnd()
	}
	s.WriteArrayEnd()
	return nil
}

// sharedLogFields holds the encoded Log members that repeat across consecutive logs, keyed by the
// values they were encoded from.
type sharedLogFields struct {
	blockNumber  hexutil.Uint64
	txHash       common.Hash
	txIndex      hexutil.Uint
	blockHash    common.Hash
	positionLen  int
	positionJSON [256]byte // the longest is 244 bytes

	removed   bool
	stamped   bool
	timestamp hexutil.Uint64
	stateLen  int
	stateJSON [64]byte // the longest is 53 bytes
}

// position returns blockNumber, transactionHash, transactionIndex and blockHash of l.
func (f *sharedLogFields) position(l *Log) []byte {
	if f.positionLen > 0 && l.BlockNumber == f.blockNumber && l.TxHash == f.txHash &&
		l.TxIndex == f.txIndex && l.BlockHash == f.blockHash {
		return f.positionJSON[:f.positionLen]
	}
	f.blockNumber, f.txHash, f.txIndex, f.blockHash = l.BlockNumber, l.TxHash, l.TxIndex, l.BlockHash
	b := append(f.positionJSON[:0], `"blockNumber":"`...)
	b = append(hexutil.AppendUint64(b, uint64(l.BlockNumber)), `","transactionHash":`...)
	b = append(hexutil.AppendQuoted(b, l.TxHash[:]), `,"transactionIndex":"`...)
	b = append(hexutil.AppendUint64(b, uint64(l.TxIndex)), `","blockHash":`...)
	f.positionLen = copy(f.positionJSON[:], hexutil.AppendQuoted(b, l.BlockHash[:]))
	return f.positionJSON[:f.positionLen]
}

// state returns removed and, when l is stamped, blockTimestamp.
func (f *sharedLogFields) state(l *Log) []byte {
	stamped := l.BlockTimestamp != nil
	if f.stateLen > 0 && l.Removed == f.removed && stamped == f.stamped &&
		(!stamped || *l.BlockTimestamp == f.timestamp) {
		return f.stateJSON[:f.stateLen]
	}
	f.removed, f.stamped = l.Removed, stamped
	b := strconv.AppendBool(append(f.stateJSON[:0], `"removed":`...), l.Removed)
	if stamped {
		f.timestamp = *l.BlockTimestamp
		b = append(hexutil.AppendUint64(append(b, `,"blockTimestamp":"`...), uint64(f.timestamp)), '"')
	}
	f.stateLen = copy(f.stateJSON[:], b)
	return f.stateJSON[:f.stateLen]
}
