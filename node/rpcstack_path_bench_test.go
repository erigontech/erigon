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

package node

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

// pathBenchEthAPI has the signatures of the real eth_chainId and eth_getBalance
// with no state behind them, so the benchmarks measure only the HTTP/RPC path.
type pathBenchEthAPI struct{}

func (pathBenchEthAPI) ChainId(context.Context) (hexutil.Uint64, error) { return 1, nil }

func (pathBenchEthAPI) GetBalance(_ context.Context, _ common.Address, _ *rpc.BlockNumberOrHash) (*hexutil.U256, error) {
	return (*hexutil.U256)(uint256.NewInt(1_000_000_000_000_000_000)), nil
}

var pathBenchBodies = []struct {
	name string
	body []byte
}{
	{"chainId", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)},
	{"getBalance", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x43d8c21431c86df8698e546b7e05f5d5d078828e","latest"]}`)},
}

// newPathBenchHandler builds the handler chain the rpcdaemon serves on its HTTP port
// with default flags: vhosts, loopback-skipping compression and admission control.
func newPathBenchHandler(b *testing.B) http.Handler {
	srv := rpc.NewServer(1, false, false, false, log.New(), 0)
	b.Cleanup(srv.Stop)
	if err := srv.RegisterName("eth", pathBenchEthAPI{}); err != nil {
		b.Fatal(err)
	}
	return NewHTTPHandlerStack(srv, nil, []string{"localhost"}, true, false, 1024, true)
}

type pathBenchResponseWriter struct{ header http.Header }

func (w *pathBenchResponseWriter) Header() http.Header         { return w.header }
func (w *pathBenchResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *pathBenchResponseWriter) WriteHeader(int)             {}

// BenchmarkRPCPathInProcess serves one request through the handler chain with no
// network: the cost of Erigon's own HTTP handlers and the RPC framework.
func BenchmarkRPCPathInProcess(b *testing.B) {
	h := newPathBenchHandler(b)
	for _, tc := range pathBenchBodies {
		b.Run(tc.name, func(b *testing.B) {
			checkPathBenchResponse(b, h, tc.body)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				reader := bytes.NewReader(tc.body)
				req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://localhost:8545/", nil)
				req.Header.Set("Content-Type", "application/json")
				req.RemoteAddr = "127.0.0.1:40000"
				w := &pathBenchResponseWriter{header: http.Header{}}
				for pb.Next() {
					reader.Reset(tc.body)
					req.Body = io.NopCloser(reader)
					req.ContentLength = int64(len(tc.body))
					clear(w.header)
					h.ServeHTTP(w, req)
				}
			})
		})
	}
}

// BenchmarkRPCPathLoopback sends keep-alive requests over loopback TCP to an
// http.Server running the same handler chain: the full server-side path,
// net/http and syscalls included, plus the cost of the Go client.
func BenchmarkRPCPathLoopback(b *testing.B) {
	ts := httptest.NewServer(newPathBenchHandler(b))
	b.Cleanup(ts.Close)
	tr := &http.Transport{MaxIdleConnsPerHost: 256}
	b.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr}
	for _, tc := range pathBenchBodies {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL, bytes.NewReader(tc.body))
					if err != nil {
						b.Error(err)
						return
					}
					req.Host = "localhost"
					req.Header.Set("Content-Type", "application/json")
					resp, err := client.Do(req)
					if err != nil {
						b.Error(err)
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						b.Errorf("status %d", resp.StatusCode)
						return
					}
				}
			})
		})
	}
}

func checkPathBenchResponse(b *testing.B, h http.Handler, body []byte) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "http://localhost:8545/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:40000"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"result":`)) {
		b.Fatalf("unexpected response %d: %s", rec.Code, rec.Body.String())
	}
}
