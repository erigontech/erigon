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
		w.WriteNextField("topics")
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
		w.WriteNextField("data")
		w.WriteHex(l.Data)
		w.WriteNextField("blockNumber")
		w.WriteHexUint64(uint64(l.BlockNumber))
		w.WriteNextField("transactionHash")
		w.WriteHex(l.TxHash[:])
		w.WriteNextField("transactionIndex")
		w.WriteHexUint64(uint64(l.TxIndex))
		w.WriteNextField("blockHash")
		w.WriteHex(l.BlockHash[:])
		w.WriteNextField("logIndex")
		w.WriteHexUint64(uint64(l.Index))
		w.WriteNextField("removed")
		w.WriteBool(l.Removed)
		w.WriteNextField("blockTimestamp")
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
