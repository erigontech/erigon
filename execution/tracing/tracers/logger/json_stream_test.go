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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/holiman/uint256"

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
func (m *mockIBS) GetState(accounts.Address, accounts.StorageKey) (uint256.Int, error) {
	return uint256.Int{}, nil
}
func (m *mockIBS) Exist(accounts.Address) (bool, error) { return false, nil }
func (m *mockIBS) GetRefund() uint64                    { return 0 }

// captureOnOpcode runs a single OnOpcode call and returns the parsed structLog entry.
// It closes the stream the same way ExecuteTraceTx does after execution.
// storageKey/storageVal are pushed onto the stack for SSTORE (top=key, below=val).
func captureOnOpcode(t *testing.T, cfg *LogConfig, memory []byte, storageKey, storageVal *common.Hash) map[string]json.RawMessage {
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

	l.OnOpcode(0, byte(op), 100, 3, scope, nil, 1, nil)

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
			obj := captureOnOpcode(t, nil, tt.memory, nil, nil)
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
