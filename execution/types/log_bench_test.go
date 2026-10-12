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

func BenchmarkLogsMarshalFastJSON(b *testing.B) {
	topic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	logs := make(Logs, 1000)
	for i := range logs {
		logs[i] = &Log{Topics: []common.Hash{topic, {}, {}}, Data: make([]byte, 32)}
	}
	b.Run("stream", func(b *testing.B) {
		b.ReportAllocs()
		rec := httptest.NewRecorder()
		for b.Loop() {
			rec.Body.Reset()
			s := jsonstream.Get(rec)
			if err := logs.MarshalFastJSONTo(s); err != nil {
				b.Fatal(err)
			}
			if err := s.Flush(); err != nil {
				b.Fatal(err)
			}
			jsonstream.Put(s)
		}
	})
	b.Run("reflect", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(logs); err != nil {
				b.Fatal(err)
			}
		}
	})
}
