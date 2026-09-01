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

package logger

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// mockOpContext implements tracing.OpContext for tests.
type mockOpContext struct {
	memory  []byte
	stack   []uint256.Int
	address accounts.Address
}

func (m *mockOpContext) MemoryData() []byte          { return m.memory }
func (m *mockOpContext) StackData() []uint256.Int    { return m.stack }
func (m *mockOpContext) Caller() accounts.Address    { return m.address }
func (m *mockOpContext) Address() accounts.Address   { return m.address }
func (m *mockOpContext) CallValue() uint256.Int      { return uint256.Int{} }
func (m *mockOpContext) CallInput() []byte           { return nil }
func (m *mockOpContext) Code() []byte                { return nil }
func (m *mockOpContext) CodeHash() accounts.CodeHash { return accounts.CodeHash{} }

// mockIBS implements tracing.IntraBlockState for tests.
type mockIBS struct{}

func (m *mockIBS) GetBalance(accounts.Address) (uint256.Int, error) { return uint256.Int{}, nil }
func (m *mockIBS) GetNonce(accounts.Address) (uint64, error)        { return 0, nil }
func (m *mockIBS) GetCode(accounts.Address) ([]byte, error)         { return nil, nil }
func (m *mockIBS) GetCodeHash(accounts.Address) (accounts.CodeHash, error) {
	return accounts.NilCodeHash, nil
}
func (m *mockIBS) GetState(accounts.Address, accounts.StorageKey) (uint256.Int, error) {
	return uint256.Int{}, nil
}
func (m *mockIBS) Exist(accounts.Address) (bool, error) { return false, nil }
func (m *mockIBS) GetRefund() uint64                    { return 0 }

// captureOnOpcode runs a single OnOpcode call and returns the parsed structLog entry.
// It closes the stream the same way ExecuteTraceTx does after execution.
// storageKey/storageVal are pushed onto the stack for SSTORE (top=key, below=val).
func captureOnOpcode(t *testing.T, cfg *LogConfig, memory []byte, storageKey, storageVal *common.Hash) map[string]json.RawMessage {
	return captureOnOpcodeWithReturnData(t, cfg, memory, nil, storageKey, storageVal)
}

func captureOnOpcodeWithReturnData(t *testing.T, cfg *LogConfig, memory []byte, rData []byte, storageKey, storageVal *common.Hash) map[string]json.RawMessage {
	t.Helper()
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	l := NewJsonStreamLogger(cfg, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	scope := &mockOpContext{memory: memory}

	op := vm.MLOAD
	if storageKey != nil {
		op = vm.SSTORE
		// SSTORE reads stack[top-1]=address, stack[top-2]=value.
		var key, val uint256.Int
		key.SetBytes(storageKey[:])
		val.SetBytes(storageVal[:])
		scope.stack = []uint256.Int{val, key} // bottom=val, top=key
	}

	l.OnOpcode(0, byte(op), 100, 3, scope, rData, 1, nil)

	// Mirror what ExecuteTraceTx does to close the stream after execution.
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	stream.Flush()

	// Parse the outer object and extract the first structLog entry.
	var outer struct {
		StructLogs []map[string]json.RawMessage `json:"structLogs"`
	}
	if err := json.Unmarshal(buf.Bytes(), &outer); err != nil {
		t.Fatalf("invalid JSON output: %v\nraw: %s", err, buf.Bytes())
	}
	if len(outer.StructLogs) == 0 {
		t.Fatal("no structLog entry in output")
	}
	return outer.StructLogs[0]
}

// captureOnOpcodes runs n OnOpcode calls through a single logger and returns every
// structLog entry that made it to the stream. The pc of each step is set to its
// index so callers can assert which steps were kept.
func captureOnOpcodes(t *testing.T, cfg *LogConfig, n int) []map[string]json.RawMessage {
	t.Helper()
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	l := NewJsonStreamLogger(cfg, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	scope := &mockOpContext{}
	for i := range n {
		l.OnOpcode(uint64(i), byte(vm.MLOAD), 100, 3, scope, nil, 1, nil)
	}

	// Mirror what ExecuteTraceTx does to close the stream after execution.
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	stream.Flush()

	var outer struct {
		StructLogs []map[string]json.RawMessage `json:"structLogs"`
	}
	if err := json.Unmarshal(buf.Bytes(), &outer); err != nil {
		t.Fatalf("invalid JSON output: %v\nraw: %s", err, buf.Bytes())
	}
	return outer.StructLogs
}

// TestJsonStreamLogger_Limit verifies that LogConfig.Limit caps the number of
// structLog entries emitted by the opcode logger, as required by the TraceConfig
// `limit` field in execution-apis (src/schemas/opcode-tracer.yaml):
//
//	"Maximum number of opcode steps to capture. Zero means no limit. When the
//	 limit is reached, execution continues but no further StructLog entries are
//	 recorded."
func TestJsonStreamLogger_Limit(t *testing.T) {
	const steps = 5
	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{"limit zero means unlimited", 0, steps},
		{"limit of one keeps a single entry", 1, 1},
		{"limit below step count truncates", 2, 2},
		{"limit equal to step count keeps all", steps, steps},
		{"limit above step count keeps all", steps + 3, steps},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureOnOpcodes(t, &LogConfig{Limit: tt.limit}, steps)
			if len(logs) != tt.want {
				t.Fatalf("structLogs count: got %d, want %d", len(logs), tt.want)
			}
			// The limit truncates the tail: the entries kept must be the first
			// ones executed, in order.
			for i, entry := range logs {
				var pc uint64
				if err := json.Unmarshal(entry["pc"], &pc); err != nil {
					t.Fatalf("cannot parse pc of entry %d: %v", i, err)
				}
				if pc != uint64(i) {
					t.Errorf("entry %d: got pc %d, want %d", i, pc, i)
				}
			}
		})
	}
}

// TestJsonStreamLogger_LimitDoesNotCorruptJSON verifies that suppressed steps emit
// nothing at all — in particular no dangling separator that would break the array.
func TestJsonStreamLogger_LimitDoesNotCorruptJSON(t *testing.T) {
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	l := NewJsonStreamLogger(&LogConfig{Limit: 1}, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	scope := &mockOpContext{}
	for i := range 4 {
		l.OnOpcode(uint64(i), byte(vm.MLOAD), 100, 3, scope, nil, 1, nil)
	}
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	stream.Flush()

	if !json.Valid(buf.Bytes()) {
		t.Fatalf("output is not valid JSON: %s", buf.Bytes())
	}
}

// closeStreamLikeCaller writes the tail ExecuteTraceTx appends once execution is
// over: exactly one array end and one object end, whatever the logger emitted.
func closeStreamLikeCaller(stream jsonstream.Stream) {
	stream.WriteArrayEnd()
	stream.WriteMore()
	stream.WriteObjectField("gas")
	stream.WriteUint64(0)
	stream.WriteMore()
	stream.WriteObjectField("failed")
	stream.WriteBool(false)
	stream.WriteObjectEnd()
}

// The structLogs prologue must be written at most once. OnExit opens it too, for
// traces that captured no step, and the caller closes exactly one object and one
// array however many frames exited.
func TestJsonStreamLogger_PrologueWrittenOnce(t *testing.T) {
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		cfg     *LogConfig
		ctx     context.Context
		opcodes int
	}{
		{"a negative limit suppresses every step", &LogConfig{Limit: -1}, context.Background(), 3},
		{"a dead context suppresses every step", &LogConfig{}, dead, 3},
		{"nothing to capture", &LogConfig{}, context.Background(), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			stream := jsonstream.New(&buf)
			l := NewJsonStreamLogger(tt.cfg, tt.ctx, stream)
			l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

			scope := &mockOpContext{}
			for i := range tt.opcodes {
				l.OnOpcode(uint64(i), byte(vm.MLOAD), 100, 3, scope, nil, 1, nil)
			}
			// Two frames exit, as in any trace of a transaction that makes a call.
			l.OnExit(1, nil, 0, nil, false)
			l.OnExit(0, nil, 0, nil, false)

			closeStreamLikeCaller(stream)
			require.NoError(t, stream.Flush())
			require.True(t, json.Valid(buf.Bytes()), "output is not valid JSON: %s", buf.Bytes())
		})
	}
}

// TestJsonStreamLogger_MemoryEncoding verifies that memory words are emitted as
// 0x-prefixed 64-char hex strings and that a partial last word is padded to 32 bytes.
func TestJsonStreamLogger_MemoryEncoding(t *testing.T) {
	zeros64 := "0x" + strings.Repeat("00", 32)
	tests := []struct {
		name   string
		memory []byte
		want   []string
	}{
		{
			name:   "full 32-byte word",
			memory: bytes.Repeat([]byte{0xab}, 32),
			want:   []string{"0x" + strings.Repeat("ab", 32)},
		},
		{
			name:   "partial last word padded to 32 bytes",
			memory: []byte{0xaa, 0xbb},
			want:   []string{"0xaabb" + strings.Repeat("00", 30)},
		},
		{
			name: "two full words",
			memory: func() []byte {
				b := make([]byte, 64)
				b[0] = 0x01
				b[32] = 0x02
				return b
			}(),
			want: []string{
				"0x01" + strings.Repeat("00", 31),
				"0x02" + strings.Repeat("00", 31),
			},
		},
		{
			name: "two full words plus partial third",
			memory: func() []byte {
				b := make([]byte, 65)
				b[64] = 0xff
				return b
			}(),
			want: []string{
				zeros64,
				zeros64,
				"0xff" + strings.Repeat("00", 31),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := captureOnOpcode(t, &LogConfig{EnableMemory: true}, tt.memory, nil, nil)
			raw, ok := obj["memory"]
			if !ok {
				t.Fatal("missing 'memory' field")
			}
			var got []string
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("cannot parse memory: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("memory word count: got %d, want %d\ngot: %v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("word[%d]: got %s, want %s", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestJsonStreamLogger_StorageEncoding verifies that storage keys and values are
// emitted with the 0x prefix.
func TestJsonStreamLogger_StorageEncoding(t *testing.T) {
	key := common.BigToHash(common.Big1)
	val := common.BigToHash(common.Big2)

	obj := captureOnOpcode(t, nil, nil, &key, &val)

	raw, ok := obj["storage"]
	if !ok {
		t.Fatal("missing 'storage' field")
	}
	var storage map[string]string
	if err := json.Unmarshal(raw, &storage); err != nil {
		t.Fatalf("cannot parse storage: %v", err)
	}
	wantKey := "0x0000000000000000000000000000000000000000000000000000000000000001"
	wantVal := "0x0000000000000000000000000000000000000000000000000000000000000002"
	gotVal, found := storage[wantKey]
	if !found {
		t.Fatalf("storage key %s not found; got: %v", wantKey, storage)
	}
	if gotVal != wantVal {
		t.Errorf("storage value: got %s, want %s", gotVal, wantVal)
	}
}

// TestJsonStreamLogger_EnableMemory verifies that the memory field is present when
// EnableMemory is true and absent when false (the default).
func TestJsonStreamLogger_EnableMemory(t *testing.T) {
	mem := bytes.Repeat([]byte{0xab}, 32)

	t.Run("enableMemory=true includes memory field", func(t *testing.T) {
		obj := captureOnOpcode(t, &LogConfig{EnableMemory: true}, mem, nil, nil)
		if _, ok := obj["memory"]; !ok {
			t.Error("expected 'memory' field to be present, but it was absent")
		}
	})

	t.Run("enableMemory=false excludes memory field", func(t *testing.T) {
		obj := captureOnOpcode(t, &LogConfig{EnableMemory: false}, mem, nil, nil)
		if _, ok := obj["memory"]; ok {
			t.Error("expected 'memory' field to be absent, but it was present")
		}
	})

	t.Run("enableMemory=true with empty memory excludes memory field", func(t *testing.T) {
		obj := captureOnOpcode(t, &LogConfig{EnableMemory: true}, nil, nil, nil)
		if _, ok := obj["memory"]; ok {
			t.Error("expected 'memory' field to be absent when memory is empty, but it was present")
		}
	})
}

// TestJsonStreamLogger_EnableReturnData verifies that the returnData field is present
// when EnableReturnData is true and absent when false (the default).
func TestJsonStreamLogger_EnableReturnData(t *testing.T) {
	rData := []byte{0xde, 0xad, 0xbe, 0xef}

	t.Run("enableReturnData=true includes returnData field", func(t *testing.T) {
		obj := captureOnOpcodeWithReturnData(t, &LogConfig{EnableReturnData: true}, nil, rData, nil, nil)
		raw, ok := obj["returnData"]
		if !ok {
			t.Fatal("expected 'returnData' field to be present, but it was absent")
		}
		var got string
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("cannot parse returnData: %v", err)
		}
		if got != "0xdeadbeef" {
			t.Errorf("returnData: got %s, want 0xdeadbeef", got)
		}
	})

	t.Run("enableReturnData=false excludes returnData field", func(t *testing.T) {
		obj := captureOnOpcodeWithReturnData(t, &LogConfig{EnableReturnData: false}, nil, rData, nil, nil)
		if _, ok := obj["returnData"]; ok {
			t.Error("expected 'returnData' field to be absent, but it was present")
		}
	})
}

// TestStructLog_ErrorOmitempty verifies that the 'error' field is omitted from
// MarshalJSON output when there is no error, and present when there is.
func TestStructLog_ErrorOmitempty(t *testing.T) {
	t.Run("no error omitted", func(t *testing.T) {
		log := StructLog{Pc: 1, Op: vm.STOP, Gas: 10, GasCost: 1, Depth: 1}
		b, err := log.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(b, &obj); err != nil {
			t.Fatal(err)
		}
		if _, found := obj["error"]; found {
			t.Errorf("expected 'error' field to be absent, but it was present: %s", obj["error"])
		}
	})

	t.Run("error included when present", func(t *testing.T) {
		log := StructLog{Pc: 1, Op: vm.STOP, Gas: 10, GasCost: 1, Depth: 1, Err: errors.New("out of gas")}
		b, err := log.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(b, &obj); err != nil {
			t.Fatal(err)
		}
		raw, found := obj["error"]
		if !found {
			t.Fatal("expected 'error' field but it was absent")
		}
		var msg string
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("cannot parse error field: %v", err)
		}
		if msg != "out of gas" {
			t.Errorf("error message: got %q, want %q", msg, "out of gas")
		}
	})
}

// TestJsonStreamLogger_StorageEncodingManyKeys covers the separator handling when
// more than one slot is emitted; a single-entry object never writes one.
func TestJsonStreamLogger_StorageEncodingManyKeys(t *testing.T) {
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	l := NewJsonStreamLogger(&LogConfig{}, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	scope := &mockOpContext{}
	want := map[string]string{}
	for i := range 4 {
		key := common.BigToHash(big.NewInt(int64(i + 1)))
		val := common.BigToHash(big.NewInt(int64(100 + i)))
		scope.stack = []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])}
		l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
		want["0x"+hex.EncodeToString(key[:])] = "0x" + hex.EncodeToString(val[:])
	}

	// Close the way the production epilogue does: ClosePending would repair an
	// imbalance into valid JSON and hide exactly what this pins.
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	require.NoError(t, stream.Flush())
	require.True(t, json.Valid(buf.Bytes()), "output is not valid JSON: %s", buf.Bytes())

	var out struct {
		StructLogs []struct {
			Storage map[string]string `json:"storage"`
		} `json:"structLogs"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	require.NotEmpty(t, out.StructLogs)

	// Storage accumulates, so the last step carries every pair and exercises
	// three separators.
	require.Equal(t, want, out.StructLogs[len(out.StructLogs)-1].Storage)
}

// TestJsonStreamLogger_StorageWithMemory drives both users of hexEncodeBuf in a
// single step, which is what the aliasing in hexWithPrefix depends on.
func TestJsonStreamLogger_StorageWithMemory(t *testing.T) {
	key := common.BigToHash(common.Big1)
	val := common.BigToHash(common.Big2)
	scope := &mockOpContext{
		memory: bytes.Repeat([]byte{0xcd}, 64),
		stack:  []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])},
	}

	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	l := NewJsonStreamLogger(&LogConfig{EnableMemory: true}, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}
	l.OnOpcode(0, byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	require.NoError(t, stream.Flush())

	var out struct {
		StructLogs []struct {
			Memory  []string          `json:"memory"`
			Storage map[string]string `json:"storage"`
		} `json:"structLogs"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out), "output: %s", buf.Bytes())
	require.Len(t, out.StructLogs, 1)

	word := "0x" + strings.Repeat("cd", 32)
	require.Equal(t, []string{word, word}, out.StructLogs[0].Memory)
	require.Equal(t, map[string]string{
		"0x" + hex.EncodeToString(key[:]): "0x" + hex.EncodeToString(val[:]),
	}, out.StructLogs[0].Storage)
}

// TestJsonStreamLogger_ClosePendingAfterMemory pins that writing memory words keeps
// the stream's auto-close stack balanced: one word spans three stream calls, and
// only the first may consume a pending comma or field.
func TestJsonStreamLogger_ClosePendingAfterMemory(t *testing.T) {
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	l := NewJsonStreamLogger(&LogConfig{EnableMemory: true}, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	scope := &mockOpContext{memory: bytes.Repeat([]byte{0xab}, 32*4)}
	for i := range 3 {
		l.OnOpcode(uint64(i), byte(vm.MLOAD), 100, 3, scope, nil, 1, nil)
	}

	require.NoError(t, stream.ClosePending(0))
	require.NoError(t, stream.Flush())
	require.True(t, json.Valid(buf.Bytes()), "output is not valid JSON: %s", buf.Bytes())
}

func BenchmarkJsonStreamLogger_OnOpcode(b *testing.B) {
	key := common.BigToHash(common.Big1)
	val := common.BigToHash(common.Big2)
	scope := &mockOpContext{
		memory: bytes.Repeat([]byte{0xab}, 256),
		stack:  []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])},
	}

	stream := jsonstream.New(io.Discard)
	l := NewJsonStreamLogger(&LogConfig{EnableMemory: true}, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
		i++
	}
}

// countingWriter discards but records how much a response actually produced,
// and how many separate Write calls it took to deliver it.
type countingWriter struct {
	n      int64
	writes int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	w.writes++
	return len(p), nil
}

// largeTrace drives the logger over enough opcodes to produce a response far
// larger than any buffer, reporting the bytes written, the peak buffer, and
// how many separate writes reached the underlying io.Writer.
func largeTrace(tb testing.TB, steps int, cfg *LogConfig) (produced int64, peakBuffer, writes int) {
	tb.Helper()
	var out countingWriter
	stream := jsonstream.New(&out)
	l := NewJsonStreamLogger(cfg, context.Background(), stream)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

	key := common.BigToHash(common.Big1)
	val := common.BigToHash(common.Big2)
	scope := &mockOpContext{
		memory: bytes.Repeat([]byte{0xab}, 4096),
		stack:  []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])},
	}
	for i := range steps {
		l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
		if n := len(stream.Buffer()); n > peakBuffer {
			peakBuffer = n
		}
	}
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()
	require.NoError(tb, stream.Flush())
	return out.n, peakBuffer, out.writes
}

// TestJsonStreamLogger_LargeTraceStaysBounded covers the case streaming exists
// for: one transaction whose trace dwarfs any buffer. Nothing in the RPC layer
// flushes inside a transaction, so the stream has to do it. Memory tracing used
// to bound it by accident, because writeMemoryWordRaw went through Write, which
// flushed per 32-byte word; with memory off nothing drained at all.
func TestJsonStreamLogger_LargeTraceStaysBounded(t *testing.T) {
	for name, cfg := range map[string]*LogConfig{
		"storage only":   {},
		"memory enabled": {EnableMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			steps := 20_000
			produced, peak, writes := largeTrace(t, steps, cfg)
			require.Greater(t, produced, int64(1<<20), "the trace must dwarf the buffer to mean anything")
			require.Less(t, peak, 4*jsonstream.FlushThreshold,
				"buffer peaked at %d for a %dMB response", peak, produced>>20)
			require.Less(t, writes, steps,
				"%d writes for %d steps: memory words must batch through the buffer, not forward one write per word", writes, steps)
		})
	}
}

// TestHexQuotedMatchesUint256Hex pins the stack encoding against uint256.Hex,
// which the RPC output has to stay byte-identical to. The interesting cases are
// the nibble boundaries: Hex counts nibbles, not bytes, so 0xf is one digit and
// 0x10 is two.
func TestHexQuotedMatchesUint256Hex(t *testing.T) {
	l := &JsonStreamLogger{}
	for _, str := range []string{
		"0", "1", "f", "10", "ff", "100",
		"1234567890abcdef",
		"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"0000000000000000000000000000000000000000000000000000000000000001",
		"8000000000000000000000000000000000000000000000000000000000000000",
	} {
		v := new(uint256.Int).SetBytes(common.FromHex("0x" + str))
		require.Equal(t, `"`+v.Hex()+`"`, l.hexQuoted(v), "value 0x%s", str)
	}
}

// BenchmarkOnOpcodeStackDepth shows the scaling: the saved allocation is per
// stack slot per step, and a real trace is not two slots deep.
func BenchmarkOnOpcodeStackDepth(b *testing.B) {
	for _, depth := range []int{2, 8, 16, 32} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			stack := make([]uint256.Int, depth)
			for i := range stack {
				stack[i].SetUint64(uint64(i)*0x0123456789abcdef + 1)
			}
			scope := &mockOpContext{memory: bytes.Repeat([]byte{0xab}, 256), stack: stack}
			l := NewJsonStreamLogger(&LogConfig{}, context.Background(), jsonstream.New(io.Discard))
			l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

			b.ReportAllocs()
			i := 0
			for b.Loop() {
				l.OnOpcode(uint64(i), byte(vm.ADD), 100, 3, scope, nil, 1, nil)
				i++
				// Nothing else drains this stream, and every iteration appends to it.
				_ = l.stream.Flush()
			}
		})
	}
}

func BenchmarkStackValueWrite(b *testing.B) {
	vals := make([]uint256.Int, 16)
	for i := range vals {
		vals[i].SetUint64(uint64(i)*0x0123456789abcdef + 1)
	}

	b.Run("WriteString_Hex", func(b *testing.B) {
		s := jsonstream.New(io.Discard)
		b.ReportAllocs()
		for b.Loop() {
			for i := range vals {
				s.WriteString(vals[i].Hex())
			}
			_ = s.Flush()
		}
	})
	b.Run("WriteRaw_hexQuoted", func(b *testing.B) {
		l := &JsonStreamLogger{stream: jsonstream.New(io.Discard)}
		b.ReportAllocs()
		for b.Loop() {
			for i := range vals {
				l.stream.WriteRaw(l.hexQuoted(&vals[i]))
			}
			_ = l.stream.Flush()
		}
	})
}

// TestHexQuotedHashMatchesHexWithPrefix pins the pre-quoted form against the
// one WriteString produced, which the RPC output has to stay identical to.
func TestHexQuotedHashMatchesHexWithPrefix(t *testing.T) {
	l := &JsonStreamLogger{}
	for _, seed := range []int{0, 1, 7, 255} {
		var h common.Hash
		for i := range h {
			h[i] = byte(i*seed + 1)
		}
		want := `"` + l.hexWithPrefix(&h) + `"`
		require.Equal(t, want, l.hexQuotedHash(&h), "seed=%d", seed)
	}
}

// BenchmarkOnOpcodeStorage covers the shape debug_traceTransaction takes by
// default: two hex strings per touched slot, accumulating across steps.
func BenchmarkOnOpcodeStorage(b *testing.B) {
	for _, slots := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			l := NewJsonStreamLogger(&LogConfig{}, context.Background(), jsonstream.New(io.Discard))
			l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}
			scope := &mockOpContext{}
			for i := range slots {
				key := common.BigToHash(big.NewInt(int64(i + 1)))
				val := common.BigToHash(big.NewInt(int64(1000 + i)))
				scope.stack = []uint256.Int{*new(uint256.Int).SetBytes(val[:]), *new(uint256.Int).SetBytes(key[:])}
				l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				l.OnOpcode(uint64(i), byte(vm.SSTORE), 100, 3, scope, nil, 1, nil)
				i++
				// Nothing else drains this stream, and every iteration appends to it.
				_ = l.stream.Flush()
			}
		})
	}
}
