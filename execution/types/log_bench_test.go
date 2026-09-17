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
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// BenchmarkRPCLogsMarshalFastJSON compares an eth_getLogs result of 1000 logs written into an
// http.ResponseWriter by a reused json.Encoder vs MarshalFastJSONTo through a pooled stream.
func BenchmarkRPCLogsMarshalFastJSON(b *testing.B) {
	topic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	logs := make(RPCLogs, 1000)
	for i := range logs {
		logs[i] = &RPCLog{Log: Log{Topics: []common.Hash{topic, {}, {}}, Data: make([]byte, 32)}}
	}
	enc, err := json.Marshal(logs)
	if err != nil {
		b.Fatal(err)
	}
	size := int64(len(enc))

	b.Run("fast", func(b *testing.B) {
		w := httptest.NewRecorder()
		serve := func() {
			w.Body.Reset()
			s := jsonstream.Get(w)
			defer jsonstream.Put(s)
			if err := logs.MarshalFastJSONTo(s); err != nil {
				b.Fatal(err)
			}
			if err := s.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			serve()
		}
		if w.Body.Len() != int(size) {
			b.Fatalf("wrote %d bytes, want %d", w.Body.Len(), size)
		}
	})
	b.Run("reflect", func(b *testing.B) {
		w := httptest.NewRecorder()
		e := json.NewEncoder(w)
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			w.Body.Reset()
			if err := e.Encode(&logs); err != nil {
				b.Fatal(err)
			}
		}
	})
}
