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
	"fmt"
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

// marshalLogsOneGrow sizes the buffer once from the largest log, so the encode never
// grows: one allocation per response, deterministic regardless of log order.
func marshalLogsOneGrow(logs RPCLogs, w *jsonstream.StackStream) {
	maxLen := 0
	for _, l := range logs {
		if n := l.JSONLen(); n > maxLen {
			maxLen = n
		}
	}
	buf := make([]byte, 0, maxLen)
	w.WriteArrayStart()
	for i, l := range logs {
		if i > 0 {
			w.WriteMore()
		}
		buf = l.AppendJSON(buf[:0])
		w.WriteRawBytes(buf)
	}
	w.WriteArrayEnd()
}

// marshalLogsNoHint lets append find its own steady state, with no size hint at all.
func marshalLogsNoHint(logs RPCLogs, w *jsonstream.StackStream) {
	var buf []byte
	w.WriteArrayStart()
	for i, l := range logs {
		if i > 0 {
			w.WriteMore()
		}
		buf = l.AppendJSON(buf[:0])
		w.WriteRawBytes(buf)
	}
	w.WriteArrayEnd()
}

func BenchmarkRPCLogsGrowStrategy(b *testing.B) {
	topic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	for _, n := range []int{100, 1000, 10000} {
		logs := make(RPCLogs, n)
		for i := range logs {
			// vary data size so the max is not the first log
			logs[i] = &RPCLog{Log: Log{Topics: []common.Hash{topic, {}, {}}, Data: make([]byte, 32+(i%7)*16)}}
		}
		enc, _ := json.Marshal(logs)
		size := int64(len(enc))
		for _, v := range []struct {
			name string
			fn   func(RPCLogs, *jsonstream.StackStream)
		}{
			{"perLogGrow", func(l RPCLogs, s *jsonstream.StackStream) { _ = l.MarshalFastJSONTo(s) }},
			{"oneGrow", marshalLogsOneGrow},
			{"noHint", marshalLogsNoHint},
		} {
			b.Run(fmt.Sprintf("logs=%d/%s", n, v.name), func(b *testing.B) {
				w := httptest.NewRecorder()
				b.SetBytes(size)
				b.ReportAllocs()
				for b.Loop() {
					w.Body.Reset()
					s := jsonstream.Get(w)
					v.fn(logs, s)
					if err := s.Flush(); err != nil {
						b.Fatal(err)
					}
					jsonstream.Put(s)
				}
				if w.Body.Len() != int(size) {
					b.Fatalf("wrote %d bytes, want %d", w.Body.Len(), size)
				}
			})
		}
	}
}
