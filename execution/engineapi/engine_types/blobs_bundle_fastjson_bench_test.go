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
	"io"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// BenchmarkBlobsBundleMarshal compares the worst-case getPayload blobs bundle (a full mainnet block,
// 21 blobs with Osaka cell proofs) encoded by stdlib reflection vs MarshalFastJSONTo.
func BenchmarkBlobsBundleMarshal(b *testing.B) {
	bundle := worstCaseBlobsBundle()
	enc, _ := json.Marshal(bundle)
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
		s := jsonstream.Get(io.Discard)
		b.SetBytes(size)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := bundle.MarshalFastJSONTo(s); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGetPayloadResponseJSON decodes and encodes a getPayload result with a 16-transaction payload.
func BenchmarkGetPayloadResponseJSON(b *testing.B) {
	enc, err := json.Marshal(getPayloadResponse(b, `"0x3b9aca00"`, `"0x1bc16d674ec80000"`, 16))
	if err != nil {
		b.Fatal(err)
	}
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
		s := jsonstream.Get(io.Discard)
		b.ReportAllocs()
		for b.Loop() {
			if err := resp.MarshalFastJSONTo(s); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// getPayloadResponse is a getPayload result with txs 120-byte transactions. Quantities are given as JSON,
// so the fixture builds whatever type the fields have.
func getPayloadResponse(tb testing.TB, baseFee, blockValue string, txs int) *GetPayloadResponse {
	r := &GetPayloadResponse{ExecutionPayload: &ExecutionPayload{LogsBloom: make(hexutil.Bytes, 256), Transactions: make([]hexutil.Bytes, txs)}}
	for i := range r.ExecutionPayload.Transactions {
		r.ExecutionPayload.Transactions[i] = make(hexutil.Bytes, 120)
	}
	if err := json.Unmarshal([]byte(baseFee), &r.ExecutionPayload.BaseFeePerGas); err != nil {
		tb.Fatal(err)
	}
	if err := json.Unmarshal([]byte(blockValue), &r.BlockValue); err != nil {
		tb.Fatal(err)
	}
	return r
}
