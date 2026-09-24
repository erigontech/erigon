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
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// MarshalFastJSONTo writes the log in the field order encoding/json uses for the struct,
// straight into the response stream, so a result never needs a buffer of its own.
func (l *RPCLog) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	if l == nil {
		s.WriteNil()
		return nil
	}
	s.WriteObjectStart()
	jsonstream.Hex(s, "address", l.Address)
	jsonstream.Hexes(s, "topics", l.Topics)
	// A nil Data is "0x", not null.
	jsonstream.Hex(s, "data", l.Data)
	jsonstream.Hex(s, "blockNumber", l.BlockNumber)
	jsonstream.Hex(s, "transactionHash", l.TxHash)
	jsonstream.Hex(s, "transactionIndex", l.TxIndex)
	jsonstream.Hex(s, "blockHash", l.BlockHash)
	jsonstream.Hex(s, "logIndex", l.Index)
	s.Field("removed").WriteBool(l.Removed)
	jsonstream.Hex(s, "blockTimestamp", l.BlockTimestamp)
	s.WriteObjectEnd()
	return nil
}

// MarshalFastJSONTo writes the logs as a bare array. The receiver must stay a value: with a
// pointer method RPCLogs itself would not satisfy the fast-JSON interface.
func (logs RPCLogs) MarshalFastJSONTo(s *jsonstream.StackStream) error {
	jsonstream.ArrayValue(s, logs, writeLogElem)
	return nil
}

func writeLogElem(s *jsonstream.StackStream, l **RPCLog) { _ = (*l).MarshalFastJSONTo(s) }
