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

package vm

import (
	"bytes"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestMemoryCopy(t *testing.T) {
	t.Parallel()
	// Test cases from https://eips.ethereum.org/EIPS/eip-5656#test-cases
	for i, tc := range []struct {
		dst, src, len uint64
		pre           string
		want          string
	}{
		{ // MCOPY 0 32 32 - copy 32 bytes from offset 32 to offset 0.
			0, 32, 32,
			"0000000000000000000000000000000000000000000000000000000000000000 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		},

		{ // MCOPY 0 0 32 - copy 32 bytes from offset 0 to offset 0.
			0, 0, 32,
			"0101010101010101010101010101010101010101010101010101010101010101",
			"0101010101010101010101010101010101010101010101010101010101010101",
		},
		{ // MCOPY 0 1 8 - copy 8 bytes from offset 1 to offset 0 (overlapping).
			0, 1, 8,
			"000102030405060708 000000000000000000000000000000000000000000000000",
			"010203040506070808 000000000000000000000000000000000000000000000000",
		},
		{ // MCOPY 1 0 8 - copy 8 bytes from offset 0 to offset 1 (overlapping).
			1, 0, 8,
			"000102030405060708 000000000000000000000000000000000000000000000000",
			"000001020304050607 000000000000000000000000000000000000000000000000",
		},
		// Tests below are not in the EIP, but maybe should be added
		{ // MCOPY 0xFFFFFFFFFFFF 0xFFFFFFFFFFFF 0 - copy zero bytes from out-of-bounds index(overlapping).
			0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF, 0,
			"11",
			"11",
		},
		{ // MCOPY 0xFFFFFFFFFFFF 0 0 - copy zero bytes from start of mem to out-of-bounds.
			0xFFFFFFFFFFFF, 0, 0,
			"11",
			"11",
		},
		{ // MCOPY 0 0xFFFFFFFFFFFF 0 - copy zero bytes from out-of-bounds to start of mem
			0, 0xFFFFFFFFFFFF, 0,
			"11",
			"11",
		},
	} {
		m := NewMemory()
		// Clean spaces
		data := common.FromHex(strings.ReplaceAll(tc.pre, " ", ""))
		// Set pre
		m.Resize(uint64(len(data)))
		m.Set(0, uint64(len(data)), data)
		// Do the copy
		m.Copy(tc.dst, tc.src, tc.len)
		want := common.FromHex(strings.ReplaceAll(tc.want, " ", ""))
		if have := m.store; !bytes.Equal(want, have) {
			t.Errorf("case %d: want: %#x\nhave: %#x\n", i, want, have)
		}
	}
}

// TestMemoryResizeZeroesReusedBuffer pins the fast path Resize takes once the
// buffer is warm: a frame must not see the previous frame's bytes.
func TestMemoryResizeZeroesReusedBuffer(t *testing.T) {
	t.Parallel()
	var m Memory
	m.Resize(64)
	for i := range m.store {
		m.store[i] = 0xFF
	}
	m.reset()

	m.Resize(64)
	if want := make([]byte, 64); !bytes.Equal(m.store, want) {
		t.Fatalf("reused buffer not zeroed: %#x", m.store)
	}
}

func TestMemoryResizeCapacity(t *testing.T) {
	t.Parallel()
	var m Memory
	m.Resize(32)
	if got := cap(m.store); got != memoryPageSize {
		t.Fatalf("cold resize to 32: cap %d, want %d", got, memoryPageSize)
	}

	m.Resize(memoryPageSize + 32)
	if got := cap(m.store); got != 2*memoryPageSize {
		t.Fatalf("grow past one page: cap %d, want %d", got, 2*memoryPageSize)
	}

	// Doubling wins over page alignment here: align-only would give 3 pages.
	m.Resize(2*memoryPageSize + 32)
	if got := cap(m.store); got != 4*memoryPageSize {
		t.Fatalf("grow past two pages: cap %d, want %d", got, 4*memoryPageSize)
	}
}

// TestMemoryResizeForMatchesResize runs memory-writing ops on a reused dirty
// buffer and checks every byte the op did not write still reads as zero.
func TestMemoryResizeForMatchesResize(t *testing.T) {
	t.Parallel()
	seq := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i + 1)
		}
		return b
	}
	for _, tc := range []struct {
		name    string
		op      OpCode
		oldLen  uint64
		stack   []uint64 // top first
		memSize uint64
	}{
		{"mstore aligned", MSTORE, 32, []uint64{32, 7}, 64},
		{"mstore unaligned", MSTORE, 32, []uint64{40, 7}, 96},
		{"mstore beyond end", MSTORE, 0, []uint64{100, 7}, 160},
		{"mstore8", MSTORE8, 0, []uint64{37, 7}, 64},
		{"calldatacopy", CALLDATACOPY, 0, []uint64{0, 0, 40}, 64},
		{"calldatacopy short source", CALLDATACOPY, 0, []uint64{3, 0, 90}, 96},
		{"calldatacopy offset past source", CALLDATACOPY, 32, []uint64{50, 1000, 20}, 96},
		{"codecopy short source", CODECOPY, 0, []uint64{5, 10, 70}, 96},
		{"returndatacopy", RETURNDATACOPY, 0, []uint64{9, 2, 30}, 64},
		{"returndatacopy out of bounds", RETURNDATACOPY, 0, []uint64{9, 30, 30}, 64},
		{"mcopy from old memory", MCOPY, 64, []uint64{70, 0, 40}, 128},
		{"mcopy from new memory", MCOPY, 0, []uint64{0, 16, 32}, 64},
		{"keccak256", KECCAK256, 0, []uint64{0, 64}, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := func(dirty bool) []byte {
				evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chain.AllProtocolChanges, Config{})
				evm.returnData = seq(40)
				ctx := &CallContext{input: seq(40)}
				ctx.Contract.Code = seq(45)
				if dirty {
					ctx.Memory.Resize(memoryPageSize)
					for i := range ctx.Memory.store {
						ctx.Memory.store[i] = 0xFF
					}
					ctx.Memory.reset()
				}
				ctx.Memory.Resize(tc.oldLen)
				for i := len(tc.stack) - 1; i >= 0; i-- {
					ctx.Stack.push(*uint256.NewInt(tc.stack[i]))
				}
				if dirty {
					ctx.Memory.resizeFor(tc.memSize, tc.op, &ctx.Stack, len(evm.returnData))
				} else {
					ctx.Memory.Resize(tc.memSize)
				}
				_, _, _ = evm.jt[tc.op].execute(0, evm, ctx)
				return bytes.Clone(ctx.Memory.store)
			}
			if want, have := run(false), run(true); !bytes.Equal(want, have) {
				t.Fatalf("want %x\nhave %x", want, have)
			}
		})
	}
}
