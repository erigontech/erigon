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

package jsonrpc

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
)

func syntheticWitness(totalBytes, avgNode int) *ExecutionWitnessResult {
	n := totalBytes / avgNode
	st := make([]hexutil.Bytes, n)
	for i := range st {
		buf := make([]byte, avgNode)
		for j := range buf {
			buf[j] = byte(i*131 + j*7)
		}
		st[i] = buf
	}
	return &ExecutionWitnessResult{State: st, Codes: []hexutil.Bytes{}}
}

// BenchmarkWitnessServeOnDemand is the serialization the rpc layer runs when it
// serves a freshly built result: MarshalFastJSON marshals the struct fields.
func BenchmarkWitnessServeOnDemand(b *testing.B) {
	for _, mb := range []int{6, 15, 25} {
		w := syntheticWitness(mb*1_000_000, 200)
		b.Run(fmt.Sprintf("%dMB", mb), func(b *testing.B) {
			b.ReportAllocs()
			var out int
			for i := 0; i < b.N; i++ {
				buf, err := w.MarshalFastJSON()
				if err != nil {
					b.Fatal(err)
				}
				out = len(buf)
			}
			b.ReportMetric(float64(out)/1e6, "MB_json")
		})
	}
}

// BenchmarkWitnessServeCacheHit is what a cache hit serves: the builder marshaled
// the JSON once (off-path), so MarshalFastJSON on the stored shell returns those
// bytes verbatim — no per-hit marshal.
func BenchmarkWitnessServeCacheHit(b *testing.B) {
	for _, mb := range []int{6, 15, 25} {
		enc, err := syntheticWitness(mb*1_000_000, 200).MarshalFastJSON()
		if err != nil {
			b.Fatal(err)
		}
		shell := &ExecutionWitnessResult{cachedJSON: enc}
		b.Run(fmt.Sprintf("%dMB", mb), func(b *testing.B) {
			b.ReportAllocs()
			var out int
			for i := 0; i < b.N; i++ {
				buf, err := shell.MarshalFastJSON()
				if err != nil {
					b.Fatal(err)
				}
				out = len(buf)
			}
			b.ReportMetric(float64(out)/1e6, "MB_json")
		})
	}
}
