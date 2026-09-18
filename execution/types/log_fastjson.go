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
	"github.com/erigontech/erigon/rpc/jsonstream/jsonw"
)

// MarshalFastJSONTo writes the log in the field order encoding/json uses for the struct, so the output is
// byte-identical to reflection-based marshalling.
func (l *RPCLog) MarshalFastJSONTo(w jsonw.JSONWriter) error {
	if l == nil {
		w.WriteNil()
		return nil
	}
	w.WriteObjectStart()
	{
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
		w.WriteHexUint64(uint64(l.BlockNumber))
		w.WriteMore()
		w.WriteObjectField("transactionHash")
		w.WriteHex(l.TxHash[:])
		w.WriteMore()
		w.WriteObjectField("transactionIndex")
		w.WriteHexUint64(uint64(l.TxIndex))
		w.WriteMore()
		w.WriteObjectField("blockHash")
		w.WriteHex(l.BlockHash[:])
		w.WriteMore()
		w.WriteObjectField("logIndex")
		w.WriteHexUint64(uint64(l.Index))
		w.WriteMore()
		w.WriteObjectField("removed")
		w.WriteBool(l.Removed)
		w.WriteMore()
		w.WriteObjectField("blockTimestamp")
		w.WriteHexUint64(uint64(l.BlockTimestamp))
	}
	w.WriteObjectEnd()
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
		if err := l.MarshalFastJSONTo(w); err != nil {
			return err
		}
	}
	w.WriteArrayEnd()
	return nil
}
