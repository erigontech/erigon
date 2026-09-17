// Copyright 2024 The Erigon Authors
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

//go:build go1.27

package hexutil_test

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

func BenchmarkUnmarshalBig(b *testing.B) {
	input := []byte(`"0x123456789abcdef123456789abcdef"`)
	for b.Loop() {
		var v hexutil.Big
		if err := v.UnmarshalJSON(input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkU256AppendText(b *testing.B) {
	buf := make([]byte, 0, 66)
	for _, tc := range []struct {
		name string
		v    *uint256.Int
	}{
		{"small", uint256.NewInt(42)},
		{"u64", uint256.NewInt(0x1234567890abcdef)},
		{"full", new(uint256.Int).SetAllOne()},
	} {
		v := hexutil.U256(*tc.v)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf, _ = v.AppendText(buf[:0])
			}
		})
	}
}

func BenchmarkUnmarshalUint64(b *testing.B) {
	input := []byte(`"0x123456789abcdf"`)
	for b.Loop() {
		var v hexutil.Uint64
		_ = v.UnmarshalJSON(input)
	}
}

// discardJSONWriter reuses one buffer, as a response stream does.
type discardJSONWriter struct{ buf []byte }

func (w *discardJSONWriter) AvailableBuffer(n int) []byte { return slices.Grow(w.buf[:0], n) }
func (w *discardJSONWriter) WriteRawBytes(v []byte)       { w.buf = v[:0] }

type codeAPI struct{ code hexutil.Bytes }

func (api *codeAPI) GetCode(context.Context, common.Address, *rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	return api.code, nil
}

type discardResponseWriter struct {
	header http.Header
	n      int
}

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) WriteHeader(int)             {}
func (w *discardResponseWriter) Write(b []byte) (int, error) { w.n += len(b); return len(b), nil }

// BenchmarkBytesMarshalJSON compares a 64KB eth_getCode result encoded by json/v2 vs MarshalFastJSONTo.
func BenchmarkBytesMarshalJSON(b *testing.B) {
	code := make(hexutil.Bytes, 64*1024)
	buf := make([]byte, 2*64*1024)
	for i := range code {
		code[i] = byte(i)
	}
	size := int64(hexutil.QuotedLen(len(code)))

	b.Run("jsonv2", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(code); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("jsonv2_encoder", func(b *testing.B) {
		enc := jsontext.NewEncoder(io.Discard)
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			if err := json.MarshalEncode(enc, code); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("fast_v0", func(b *testing.B) {
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			buf, _ = code.AppendText(buf[:0])
		}
	})

	b.Run("fast_v2", func(b *testing.B) {
		var w discardJSONWriter
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			if err := code.MarshalFastJSONTo(&w); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("fast_v2_stream", func(b *testing.B) {
		srv := rpc.NewServer(1, false, false, false, log.New(), 0)
		if err := srv.RegisterName("eth", &codeAPI{code: code}); err != nil {
			b.Fatal(err)
		}
		body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["0xdac17f958d2ee523a2206206994597c13d831ec7","latest"]}`)
		w := &discardResponseWriter{header: http.Header{}}
		serve := func() {
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			r.Header.Set("content-type", "application/json")
			srv.ServeHTTP(w, r)
		}
		serve()
		if want := len(`{"jsonrpc":"2.0","id":1,"result":}`+"\n") + int(size); w.n != want {
			b.Fatalf("response is %d bytes, want %d", w.n, want)
		}
		b.SetBytes(size)
		b.ReportAllocs()
		for b.Loop() {
			serve()
		}
	})

}
