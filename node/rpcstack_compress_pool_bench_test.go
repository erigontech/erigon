// Copyright 2025 The Erigon Authors
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
	"fmt"
	"net/http"
	"testing"

	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/gzhttp/writer"
	"github.com/klauspost/compress/gzhttp/writer/gzkp"
	"github.com/klauspost/compress/gzhttp/writer/zstdkp"
)

type discardResponseWriter struct{ header http.Header }

func (w *discardResponseWriter) Header() http.Header         { return w.header }
func (w *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}

// BenchmarkCompressorPools compares gzhttp's default writers, spelled out as
// gzhttp installs them, with our pooled factories at the same levels, without
// network I/O. Compare with
// benchstat -col /impl.
func BenchmarkCompressorPools(b *testing.B) {
	for _, enc := range []string{"gzip", "zstd"} {
		for _, size := range []int{4 << 10, 256 << 10} {
			payload := syntheticBlockJSON(size)
			for _, impl := range []string{"default", "pooled"} {
				b.Run(fmt.Sprintf("enc=%s/payload=%dKB/impl=%s", enc, size>>10, impl), func(b *testing.B) {
					gzipFactory := writer.GzipWriterFactory{Levels: gzkp.Levels, New: gzkp.NewWriter}
					zstdFactory := writer.ZstdWriterFactory{Levels: zstdkp.Levels, New: zstdkp.NewWriter}
					if impl == "pooled" {
						gzipFactory, zstdFactory = gzipWriterFactory, zstdWriterFactory
					}
					wrapper, err := gzhttp.NewWrapper(
						gzhttp.MinSize(minGzipBodySize),
						gzhttp.CompressionLevel(gzipLevel),
						gzhttp.EnableZstd(true),
						gzhttp.ZstdCompressionLevel(int(zstdLevel)),
						gzhttp.Implementation(gzipFactory),
						gzhttp.ZstdImplementation(zstdFactory),
					)
					if err != nil {
						b.Fatal(err)
					}
					handler := wrapper(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						w.Write(payload) //nolint:errcheck
					}))
					req, _ := http.NewRequestWithContext(b.Context(), http.MethodPost, "/", nil)
					req.Header.Set("Accept-Encoding", enc)

					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						w := &discardResponseWriter{header: http.Header{}}
						for pb.Next() {
							clear(w.header)
							handler.ServeHTTP(w, req)
						}
					})
				})
			}
		}
	}
}
