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

package node

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/erigontech/erigon/rpc/jsonstream"
	jsoniter "github.com/json-iterator/go"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const rpcEndpoint = "http://localhost:8545"

// historicalBlocks are mainnet blocks chosen from high-activity periods.
var historicalBlocks = []struct {
	tag  string
	desc string
}{
	{"0xA2FB00", "block 10,680,064 – DeFi Summer 2020"},
	{"0xBEBC20", "block 12,500,000 – Uniswap v3 launch era"},
	{"0xC65D58", "block 12,999,000 – EIP-1559 activation period"},
	{"0xD59F80", "block 14,000,000 – NFT peak 2022"},
	{"0xED4EC2", "block 15,552,194 – The Merge (PoS day-1)"},
	{"0x1036640", "block 17,000,000 – post-Merge 2023"},
	{"0x1249F60", "block 19,300,000 – EIP-4844 (blobs) era"},
}

// --- stdlib gzip handler (reference implementation) ---

// The stdlib writer runs at BestSpeed to match the production Klauspost level,
// so the benchmark isolates the library difference rather than mixing in a
// compression-level difference.
var stdlibGzPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return w
	},
}

func newStdlibGzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")

		gz := stdlibGzPool.Get().(*gzip.Writer)
		defer stdlibGzPool.Put(gz)
		gz.Reset(w)
		defer gz.Close() //nolint:errcheck

		next.ServeHTTP(&stdlibGzipResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
}

type stdlibGzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *stdlibGzipResponseWriter) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
}

func (w *stdlibGzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

// --- helpers ---

type latencyStats struct {
	avg, p50, p99, min, max time.Duration
	payloadKB               int
}

func (s latencyStats) String() string {
	return fmt.Sprintf("payload=%dKB avg=%-8v p50=%-8v p99=%-8v min=%-8v max=%v",
		s.payloadKB, s.avg, s.p50, s.p99, s.min, s.max)
}

func percentile(latencies []time.Duration, p int) time.Duration {
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// measureHandlerLatency benchmarks a local httptest handler serving payload.
func measureHandlerLatency(t testing.TB, payload []byte, wrap func(http.Handler) http.Handler) latencyStats {
	t.Helper()
	const requests = 500

	handler := wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload) //nolint:errcheck
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	for range 10 {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(payload))
		req.Header.Set("Accept-Encoding", "gzip")
		resp, _ := client.Do(req)
		if resp != nil {
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
		}
	}

	latencies := make([]time.Duration, 0, requests)
	for range requests {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(payload))
		req.Header.Set("Accept-Encoding", "gzip")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
		latencies = append(latencies, time.Since(start))
	}
	return computeStats(latencies, len(payload)/1024)
}

// measureRPCLatency measures end-to-end latency of a real rpcdaemon at endpoint.
func measureRPCLatency(t testing.TB, endpoint, blockTag string) latencyStats {
	t.Helper()
	const requests = 500

	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":[%q,true]}`,
		blockTag,
	)

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	// Warmup + fetch payload size
	var payloadKB int
	for i := range 10 {
		req, _ := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "gzip")
		resp, err := client.Do(req)
		if err != nil {
			t.Skipf("endpoint %s not reachable: %v", endpoint, err)
			return latencyStats{}
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if i == 0 {
			payloadKB = len(data) / 1024
		}
	}

	latencies := make([]time.Duration, 0, requests)
	for range requests {
		req, _ := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "gzip")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
		latencies = append(latencies, time.Since(start))
	}
	return computeStats(latencies, payloadKB)
}

func computeStats(latencies []time.Duration, payloadKB int) latencyStats {
	var total time.Duration
	lo, hi := latencies[0], latencies[0]
	for _, l := range latencies {
		total += l
		if l < lo {
			lo = l
		}
		if l > hi {
			hi = l
		}
	}
	return latencyStats{
		avg:       total / time.Duration(len(latencies)),
		p50:       percentile(latencies, 50),
		p99:       percentile(latencies, 99),
		min:       lo,
		max:       hi,
		payloadKB: payloadKB,
	}
}

// fetchPayload fetches raw block JSON from the node (for handler benchmarks).
func fetchPayload(t testing.TB, blockTag string) []byte {
	t.Helper()
	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":[%q,true]}`,
		blockTag,
	)
	resp, err := http.Post(rpcEndpoint, "application/json", strings.NewReader(body))
	if err != nil {
		t.Skipf("local node not reachable at %s: %v", rpcEndpoint, err)
		return nil
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if envelope.Error != nil {
		t.Skipf("node error for %s: %s", blockTag, envelope.Error.Message)
		return nil
	}
	if string(envelope.Result) == "null" {
		t.Skipf("block %s not available (result=null)", blockTag)
		return nil
	}
	return data
}

// --- Test: handler-level compression (klauspost vs stdlib, payload from node) ---

func TestGzipHandlerLatency(t *testing.T) {
	for _, blk := range historicalBlocks {
		t.Run(blk.desc, func(t *testing.T) {
			payload := fetchPayload(t, blk.tag)
			if payload == nil {
				return
			}
			kp := measureHandlerLatency(t, payload, newGzipHandler)
			std := measureHandlerLatency(t, payload, newStdlibGzipHandler)
			t.Logf("klauspost  %s", kp)
			t.Logf("stdlib     %s", std)
			t.Logf("speedup p50=%.2fx  p99=%.2fx", float64(std.p50)/float64(kp.p50), float64(std.p99)/float64(kp.p99))
		})
	}
}

// --- Test: end-to-end real rpcdaemon latency ---

// resultsFile is where we persist rpcdaemon latency results across runs.
const resultsFile = "/tmp/erigon_gzip_latency_results.txt"

func TestRPCDaemonLatency(t *testing.T) {
	var sb strings.Builder

	for _, blk := range historicalBlocks {
		t.Run(blk.desc, func(t *testing.T) {
			stat := measureRPCLatency(t, rpcEndpoint, blk.tag)
			line := fmt.Sprintf("%-52s  %s\n", blk.desc, stat)
			t.Log(line)
			sb.WriteString(line)
		})
	}

	// Append results to file with a header so we can diff two runs.
	f, err := os.OpenFile(resultsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("warning: could not write results file: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "\n=== %s ===\n", time.Now().Format("2006-01-02 15:04:05"))
	f.WriteString(sb.String())
	t.Logf("results appended to %s", resultsFile)
}

// --- Benchmarks ---

func benchmarkGzipHandler(b *testing.B, payload []byte, wrap func(http.Handler) http.Handler) {
	handler := wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload) //nolint:errcheck
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	for range 5 {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(payload))
		req.Header.Set("Accept-Encoding", "gzip")
		resp, _ := client.Do(req)
		if resp != nil {
			io.Copy(io.Discard, resp.Body) //nolint:errcheck
			resp.Body.Close()
		}
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var totalLatency time.Duration
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(payload))
		req.Header.Set("Accept-Encoding", "gzip")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
		totalLatency += time.Since(start)
	}
	b.ReportMetric(float64(totalLatency.Microseconds())/float64(b.N), "µs/req")
}

var (
	benchPayload     []byte
	benchPayloadOnce sync.Once
)

func getBenchPayload(b *testing.B) []byte {
	b.Helper()
	benchPayloadOnce.Do(func() {
		benchPayload = fetchPayload(b, "0xC65D58") // block 12,999,000
	})
	return benchPayload
}

func BenchmarkKlauspostGzipBestSpeed(b *testing.B) {
	benchmarkGzipHandler(b, getBenchPayload(b), newGzipHandler)
}

func BenchmarkStdlibGzipBestSpeed(b *testing.B) {
	benchmarkGzipHandler(b, getBenchPayload(b), newStdlibGzipHandler)
}

// syntheticBlockJSON builds a payload shaped like eth_getBlockByNumber output:
// many repeated tx objects with high-entropy hex fields, so the compressor sees
// realistic redundancy rather than a trivially compressible run.
func syntheticBlockJSON(targetBytes int) []byte {
	var b bytes.Buffer
	b.WriteString(`{"jsonrpc":"2.0","id":1,"result":{"number":"0xc65d58","transactions":[`)
	i := 0
	for b.Len() < targetBytes {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"hash":"0x%064x","from":"0x%040x","to":"0x%040x","value":"0x%x","gas":"0x%x","input":"0x%0128x"}`,
			i*2654435761, i*40503, i*2246822519, i*97, 21000+i, uint64(i)*11400714819323198485)
		i++
	}
	b.WriteString(`]}}`)
	return b.Bytes()
}

// BenchmarkGzipOneShotThroughput measures server throughput: concurrent clients,
// unlike the sequential per-request loop in rpcstack_gzip_bench_test.go which
// reports latency. -cpu controls the client concurrency.
func BenchmarkGzipOneShotThroughput(b *testing.B) {
	for _, size := range []int{16 << 10, 256 << 10, 768 << 10, 2 << 20} {
		payload := syntheticBlockJSON(size)
		b.Run(fmt.Sprintf("payload=%dKB", len(payload)>>10), func(b *testing.B) {
			handler := newGzipHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write(payload) //nolint:errcheck
			}))
			srv := httptest.NewServer(handler)
			defer srv.Close()

			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				client := &http.Client{Transport: &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 64}}
				for pb.Next() {
					req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
					req.Header.Set("Accept-Encoding", "gzip")
					resp, err := client.Do(req)
					if err != nil {
						b.Error(err)
						return
					}
					io.Copy(io.Discard, resp.Body) //nolint:errcheck
					resp.Body.Close()
				}
			})
		})
	}
}

// BenchmarkGzipStreamingThroughput drives the streaming path the way
// debug_trace* does: a jsonstream writing through the gzip middleware. It
// varies the jsoniter buffer, which is InitialBufferSize=4096 in production
// (rpc/jsonstream/factory.go) -- on a several-hundred-MB trace that is tens of
// thousands of write calls through the middleware.
func BenchmarkGzipStreamingThroughput(b *testing.B) {
	for _, gz := range []bool{false, true} {
		for _, bufSize := range []int{4096, 256 << 10} {
			for _, entries := range []int{2000, 60000} {
				name := fmt.Sprintf("gzip=%v/jsonbuf=%dKB/entries=%d", gz, bufSize>>10, entries)
				if bufSize < 1024 {
					name = fmt.Sprintf("gzip=%v/jsonbuf=%dB/entries=%d", gz, bufSize, entries)
				}
				b.Run(name, func(b *testing.B) {
					// Precomputed: formatting inside the write loop would make the
					// benchmark measure fmt rather than the streaming path.
					stackWords := make([]string, 64)
					for i := range stackWords {
						stackWords[i] = fmt.Sprintf("0x%064x", i*2654435761)
					}
					var wrote int64
					inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						// Production enters streaming mode via the gzip hook before
						// the first write; Flush is that same entry point.
						if f, ok := w.(http.Flusher); ok {
							f.Flush()
						}

						st := jsoniter.NewStream(jsoniter.ConfigDefault, w, bufSize)
						stream := jsonstream.Wrap(st)
						stream.WriteArrayStart()
						for i := range entries {
							if i > 0 {
								stream.WriteMore()
							}
							stream.WriteObjectStart()
							stream.WriteObjectField("pc")
							stream.WriteInt(i)
							stream.WriteMore()
							stream.WriteObjectField("op")
							stream.WriteString("SSTORE")
							stream.WriteMore()
							stream.WriteObjectField("stack")
							stream.WriteString(stackWords[i%len(stackWords)])
							stream.WriteObjectEnd()
						}
						stream.WriteArrayEnd()
						stream.Flush() //nolint:errcheck
						atomic.AddInt64(&wrote, int64(st.Buffered()))
					})
					var handler http.Handler = inner
					if gz {
						handler = newGzipHandler(inner)
					}
					srv := httptest.NewServer(handler)
					defer srv.Close()

					// One warmup request to learn the uncompressed size for MB/s.
					req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
					req.Header.Set("Accept-Encoding", "gzip")
					if resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req); err == nil {
						io.Copy(io.Discard, resp.Body) //nolint:errcheck
						resp.Body.Close()
					}
					b.SetBytes(int64(entries) * 96) // approx bytes of JSON per entry
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						client := &http.Client{Transport: &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 64}}
						for pb.Next() {
							req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
							req.Header.Set("Accept-Encoding", "gzip")
							resp, err := client.Do(req)
							if err != nil {
								b.Error(err)
								return
							}
							io.Copy(io.Discard, resp.Body) //nolint:errcheck
							resp.Body.Close()
						}
					})
				})
			}
		}
	}
}
