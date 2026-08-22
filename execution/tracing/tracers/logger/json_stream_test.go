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

// fuzzPlan turns a fuzzer's bytes into a trace to emit. Every field is derived
// rather than read directly so that any input produces a runnable plan.
type fuzzPlan struct {
	cfg      LogConfig
	steps    int
	stackLen int
	memLen   int
	rDataLen int
	opCode   byte
	withErr  bool
	closeAt  int
}

func planFromSeed(seed []byte) fuzzPlan {
	at := func(i int) int {
		if len(seed) == 0 {
			return 0
		}
		return int(seed[i%len(seed)])
	}
	p := fuzzPlan{
		steps:    at(0) % 24,
		stackLen: at(1) % 40,
		memLen:   at(2) % 200,
		rDataLen: at(3) % 40,
		opCode:   byte(at(4)),
		withErr:  at(5)&1 == 1,
		closeAt:  at(6) % 4,
	}
	p.cfg = LogConfig{
		EnableMemory:     at(7)&1 == 1,
		DisableStack:     at(8)&1 == 1,
		DisableStorage:   at(9)&1 == 1,
		EnableReturnData: at(10)&1 == 1,
		Limit:            at(11) % 8,
	}
	return p
}

// FuzzJsonStreamLoggerEmitsValidJSON drives the struct logger the way a trace
// does and requires the result to parse. The interesting inputs are the ones
// that stop early: a step limit, or a caller that closes the stream at a depth
// it did not open, which is what happens when tracing is abandoned part way.
func FuzzJsonStreamLoggerEmitsValidJSON(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{5, 4, 64, 8, byte(vm.SSTORE), 0, 0, 0, 0, 0, 0, 0, 0},
		{3, 2, 32, 0, byte(vm.MLOAD), 1, 1, 0, 0, 0, 1, 0, 0},
		{9, 33, 199, 39, byte(vm.ADD), 1, 3, 1, 1, 1, 1, 1, 3},
		{1, 0, 0, 0, byte(vm.STOP), 0, 2, 0, 0, 0, 0, 0, 1},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, seed []byte) {
		p := planFromSeed(seed)

		var buf bytes.Buffer
		stream := jsonstream.New(&buf)
		l := NewJsonStreamLogger(&p.cfg, t.Context(), stream)
		l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}

		stack := make([]uint256.Int, p.stackLen)
		for i := range stack {
			stack[i].SetUint64(uint64(i)*0x0123456789abcdef + 1)
		}
		scope := &mockOpContext{memory: bytes.Repeat([]byte{0xab}, p.memLen), stack: stack}
		rData := bytes.Repeat([]byte{0xcd}, p.rDataLen)

		var opErr error
		if p.withErr {
			opErr = errors.New("execution reverted")
		}
		for i := range p.steps {
			l.OnOpcode(uint64(i), p.opCode, 100, 3, scope, rData, 1, opErr)
		}

		// OnExit opens the envelope when nothing was captured, which is what the
		// tracer always does before the RPC layer closes it.
		l.OnExit(0, nil, 0, nil, false)

		// Close the way the RPC layer does. Every route has to end at depth zero,
		// but they get there differently: a finished trace closes what it opened,
		// an abandoned one leans on ClosePending to repair it, and the error path
		// closes back to the enclosing field first and finishes afterwards.
		switch p.closeAt {
		case 0:
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
		case 1:
			require.NoError(t, stream.ClosePending(0))
		case 2:
			require.NoError(t, stream.ClosePending(1))
			require.NoError(t, stream.ClosePending(0))
		default:
			require.NoError(t, stream.ClosePending(uint(stream.Depth())))
			require.NoError(t, stream.ClosePending(0))
		}
		require.NoError(t, stream.Flush())
		require.Zero(t, stream.Depth(), "stream left open")

		if buf.Len() == 0 {
			return // nothing was emitted, nothing to parse
		}
		var out any
		require.NoErrorf(t, json.Unmarshal(buf.Bytes(), &out),
			"plan %+v produced invalid JSON: %s", p, buf.String())
	})
}
