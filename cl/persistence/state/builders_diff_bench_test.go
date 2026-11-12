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

package state_accessors

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
)

func BenchmarkBuildersRowSize(b *testing.B) {
	pre := solid.NewStaticListSSZ[*cltypes.Builder](200, new(cltypes.Builder).EncodingSizeSSZ())
	post := solid.NewStaticListSSZ[*cltypes.Builder](200, new(cltypes.Builder).EncodingSizeSSZ())
	for i := range 200 {
		pre.Append(&cltypes.Builder{Balance: uint64(i + 1)})
		post.Append(&cltypes.Builder{Balance: uint64(i + 1)})
	}
	post.Get(100).Balance++

	b.Run("per-index", func(b *testing.B) {
		writer := new(BuildersDiffWriter)
		var row bytes.Buffer
		b.ReportAllocs()
		for b.Loop() {
			row.Reset()
			writer.Initialize(pre)
			if err := writer.WriteDiff(&row, post); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(row.Len()), "bytes/row")
	})

	b.Run("queue", func(b *testing.B) {
		writer := base_encoding.NewSSZQueueEncoder[*cltypes.Builder](func(a, b *cltypes.Builder) bool { return *a == *b })
		var row bytes.Buffer
		b.ReportAllocs()
		for b.Loop() {
			row.Reset()
			writer.Initialize(pre)
			if err := writer.WriteDiff(&row, post); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(row.Len()), "bytes/row")
	})
}
