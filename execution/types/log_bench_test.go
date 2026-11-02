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

// filterBenchLogs builds receipts of 8 logs each where 1 log in 4 comes from one of addrs and carries
// one of topic0s, as in eth_getLogs results.
func filterBenchLogs(n int) (logs Logs, addrs []common.Address, topic0s []common.Hash) {
	val := func(i, salt int) common.Hash { return common.Hash{byte(i), byte(i >> 8), byte(salt)} }
	addr := func(i, salt int) common.Address { return common.Address{byte(i), byte(i >> 8), byte(salt)} }
	for i := range n {
		addrs = append(addrs, addr(i, 1))
		topic0s = append(topic0s, val(i, 2))
	}
	for i := range 4096 {
		l := &Log{Address: addr(i, 3), Topics: []common.Hash{val(i, 4), val(i, 5), val(i, 6)}}
		if i%4 == 0 {
			l.Address, l.Topics[0] = addrs[i%n], topic0s[(i/4)%n]
		}
		logs = append(logs, l)
	}
	return logs, addrs, topic0s
}

func BenchmarkAppendFilteredLogs(b *testing.B) {
	for _, n := range []int{1, 2, 4, 16} {
		logs, addrs, topic0s := filterBenchLogs(n)
		for name, topics := range map[string][][]common.Hash{"address": nil, "address+topic0": {topic0s}} {
			b.Run(fmt.Sprintf("%s/n=%d", name, n), func(b *testing.B) {
				f := NewLogFilter(addrs, topics)
				dst := make(Logs, 0, len(logs))
				b.ReportAllocs()
				for b.Loop() {
					for i := 0; i < len(logs); i += 8 {
						dst = logs[i:i+8].AppendFilteredLogs(dst[:0], f, 1, 0)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(logs)), "ns/log")
			})
		}
	}
}
