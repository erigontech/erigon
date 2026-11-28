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

package ethapi

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

type benchEthService struct{}

func (benchEthService) Call(context.Context, CallArgs, *rpc.BlockNumberOrHash, *StateOverrides, *BlockOverrides) (hexutil.Bytes, error) {
	return hexutil.Bytes{}, nil
}

func (benchEthService) ChainId() hexutil.Uint64 { return 1 }

type discardResponseWriter struct{ header http.Header }

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}

// BenchmarkServeHTTP measures the rpc layer alone for the calls-tier bodies: the methods do no work.
func BenchmarkServeHTTP(b *testing.B) {
	srv := rpc.NewServer(50, false, false, false, log.New(), 0)
	defer srv.Stop()
	api := rpc.API{Namespace: "eth", Service: benchEthService{}, Typed: map[string]rpc.TypedMethod{"call": rpc.Method4(benchEthService{}.Call)}}
	if err := srv.RegisterAPI(api); err != nil {
		b.Fatal(err)
	}
	for _, bc := range []struct{ name, body string }{
		{"usdt-transfer", `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"from":"0x83B1742c70F43c4566Bd23Ef3357Dedfa50a7cBF","to":"0xdAC17F958D2ee523a2206206994597C13D831ec7","gas":"0x13880","value":"0x0","data":"0xa9059cbb0000000000000000000000004e2e397b3f692909b88ccb7dcfde1f727b0c136a00000000000000000000000000000000000000000000000000000000027dabe8"},"latest"]}`},
		{"chainId", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			body := []byte(bc.body)
			req := httptest.NewRequestWithContext(b.Context(), http.MethodPost, "/", nil)
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = int64(len(body))
			reader := bytes.NewReader(body)
			w := &discardResponseWriter{header: http.Header{}}
			b.ReportAllocs()
			for b.Loop() {
				reader.Reset(body)
				req.Body = io.NopCloser(reader)
				clear(w.header)
				srv.ServeHTTP(w, req)
			}
		})
	}
}
