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

package rpc

// Benchmarks for RPC dispatch: reflection-based path vs typed-generic path.
//
// Run with:
//
//	go test -run=^$ -bench=BenchmarkDispatch -benchmem ./rpc/
//
// The typed path eliminates:
//   - parsePositionalArguments (json.NewDecoder + reflect.New per arg)
//   - make([]reflect.Value, ...) per call
//   - reflect.Value.Call for the handler function
//   - json.Marshal of the result via *jsonrpcMessage intermediary

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// benchEthBlockNumber simulates the eth_blockNumber handler body.
// It returns a small integer so the benchmark is purely measuring dispatch.
func benchEthBlockNumber(_ context.Context) (uint64, error) {
	return 12345678, nil
}

// benchEthGetBalance simulates a two-parameter handler (address + block tag).
type benchGetBalanceParams struct {
	Address string `json:"address"`
	Block   string `json:"block"`
}

func benchEthGetBalance(_ context.Context, p benchGetBalanceParams) (uint64, error) {
	return 1000000, nil
}

// --- HTTP round-trip benchmarks ---
// These measure the full path: client encode → HTTP → server dispatch → HTTP → client decode.

func BenchmarkDispatch_Reflect_NoParams(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	logger := log.New()
	s := NewServer(50, false, false, true, logger, time.Duration(0))
	defer s.Stop()
	// Registered via the legacy reflection path.
	type svc struct{}
	s.RegisterName("bench", benchNoParamService{}) //nolint:errcheck
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var result uint64
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.Call(&result, "bench_blockNumber"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDispatch_Typed_NoParams(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	logger := log.New()
	s := NewServer(50, false, false, true, logger, time.Duration(0))
	defer s.Stop()
	RegisterMethod(s, "bench_blockNumber", func(ctx context.Context, _ struct{}) (uint64, error) {
		return benchEthBlockNumber(ctx)
	})
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var result uint64
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.Call(&result, "bench_blockNumber"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDispatch_Reflect_WithParams(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	logger := log.New()
	s := NewServer(50, false, false, true, logger, time.Duration(0))
	defer s.Stop()
	s.RegisterName("bench", benchGetBalanceService{}) //nolint:errcheck
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var result uint64
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.Call(&result, "bench_getBalance", "0xdeadbeef", "latest"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDispatch_Typed_WithParams(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	logger := log.New()
	s := NewServer(50, false, false, true, logger, time.Duration(0))
	defer s.Stop()
	RegisterMethod(s, "bench_getBalance", benchEthGetBalance)
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var result uint64
	// The typed path uses JSON object params, not positional array.
	params := benchGetBalanceParams{Address: "0xdeadbeef", Block: "latest"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.Call(&result, "bench_getBalance", params); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Unit-level dispatch benchmarks ---
// These skip the HTTP stack entirely and measure only the per-call overhead of
// the two dispatch paths:
//
//   Reflect path: parsePositionalArguments → make([]reflect.Value) → reflect.Value.Call → json.Marshal
//   Typed path:   json.Unmarshal into Params struct → direct call → json.Marshal
//
// Run with: go test -run=^$ -bench=BenchmarkDispatchUnit -benchmem ./rpc/

func BenchmarkDispatchUnit_Reflect_NoParams(b *testing.B) {
	logger := log.New()
	srv := newTestServer(logger) // registers "test" namespace with reflection
	if err := srv.RegisterName("benchunit", benchNoParamService{}); err != nil {
		b.Fatal(err)
	}
	callb := srv.services.callback("benchunit_blockNumber")
	if callb == nil {
		b.Fatal("callback not found")
	}

	params := json.RawMessage(nil) // eth_blockNumber has no params
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, err := parsePositionalArguments(params, callb.argTypes)
		if err != nil {
			b.Fatal(err)
		}
		result, err := callb.call(ctx, "benchunit_blockNumber", args, nil)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

func BenchmarkDispatchUnit_Typed_NoParams(b *testing.B) {
	inv := Method[struct{}, uint64]{
		fn: func(ctx context.Context, _ struct{}) (uint64, error) {
			return benchEthBlockNumber(ctx)
		},
	}

	params := json.RawMessage(nil)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := inv.invoke(ctx, params)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

func BenchmarkDispatchUnit_Reflect_WithParams(b *testing.B) {
	logger := log.New()
	srv := newTestServer(logger)
	if err := srv.RegisterName("benchunit", benchGetBalanceService{}); err != nil {
		b.Fatal(err)
	}
	callb := srv.services.callback("benchunit_getBalance")
	if callb == nil {
		b.Fatal("callback not found")
	}

	params := json.RawMessage(`["0xdeadbeef", "latest"]`)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, err := parsePositionalArguments(params, callb.argTypes)
		if err != nil {
			b.Fatal(err)
		}
		result, err := callb.call(ctx, "benchunit_getBalance", args, nil)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

func BenchmarkDispatchUnit_Typed_WithParams(b *testing.B) {
	inv := Method[benchGetBalanceParams, uint64]{fn: benchEthGetBalance}

	params := json.RawMessage(`[{"address":"0xdeadbeef","block":"latest"}]`)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := inv.invoke(ctx, params)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// --- Helper types for the reflection-based path ---

type benchNoParamService struct{}

func (benchNoParamService) BlockNumber(_ context.Context) (uint64, error) {
	return benchEthBlockNumber(context.Background())
}

type benchGetBalanceService struct{}

func (benchGetBalanceService) GetBalance(_ context.Context, address string, block string) (uint64, error) {
	return 1000000, nil
}
