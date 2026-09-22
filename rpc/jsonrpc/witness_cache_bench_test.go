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
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
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
// serves a freshly built result: MarshalFastJSONTo marshals the struct fields.
func BenchmarkWitnessServeOnDemand(b *testing.B) {
	for _, mb := range []int{6, 15, 25} {
		w := syntheticWitness(mb*1_000_000, 200)
		b.Run(fmt.Sprintf("%dMB", mb), func(b *testing.B) { serveWitness(b, w) })
	}
}

// BenchmarkWitnessServeCacheHit is what a cache hit serves: the builder marshaled
// the JSON once (off-path), so MarshalFastJSONTo on the stored shell writes those
// bytes verbatim — no per-hit marshal.
func BenchmarkWitnessServeCacheHit(b *testing.B) {
	for _, mb := range []int{6, 15, 25} {
		enc, err := json.Marshal(syntheticWitness(mb*1_000_000, 200))
		if err != nil {
			b.Fatal(err)
		}
		shell := &ExecutionWitnessResult{cachedJSON: enc}
		b.Run(fmt.Sprintf("%dMB", mb), func(b *testing.B) { serveWitness(b, shell) })
	}
}

// serveWitness writes w the way the rpc layer does: into a pooled stream over a writer.
func serveWitness(b *testing.B, w *ExecutionWitnessResult) {
	rec := httptest.NewRecorder()
	b.ReportAllocs()
	for b.Loop() {
		rec.Body.Reset()
		s := jsonstream.Get(rec)
		if err := w.MarshalFastJSONTo(s); err != nil {
			b.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			b.Fatal(err)
		}
		jsonstream.Put(s)
	}
	b.ReportMetric(float64(rec.Body.Len())/1e6, "MB_json")
}
