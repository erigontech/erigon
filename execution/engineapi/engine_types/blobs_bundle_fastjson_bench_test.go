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

package engine_types

import (
	"encoding/json"
	"testing"
)

// BenchmarkBlobsBundleMarshal compares the worst-case getPayload blobs bundle (a full mainnet block,
// 21 blobs with Osaka cell proofs) encoded by stdlib reflection vs MarshalFastJSON.
func BenchmarkBlobsBundleMarshal(b *testing.B) {
	bundle := worstCaseBlobsBundle()
	enc, _ := bundle.MarshalFastJSON()
	size := int64(len(enc))

	b.Run("stdlib_reflect", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(bundle); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fast", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := bundle.MarshalFastJSON(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGetPayloadResponseJSON decodes and encodes a getPayload result with a 16-transaction payload.
func BenchmarkGetPayloadResponseJSON(b *testing.B) {
	enc := []byte(getPayloadResponseJSON(`"0x3b9aca00"`, `"0x1bc16d674ec80000"`, 16))
	var resp GetPayloadResponse
	if err := json.Unmarshal(enc, &resp); err != nil {
		b.Fatal(err)
	}
	b.Run("unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var r GetPayloadResponse
			if err := json.Unmarshal(enc, &r); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := resp.MarshalFastJSON(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
