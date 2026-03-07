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

package vm

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestExecHasher_Determinism(t *testing.T) {
	// Same sequence of operations must produce the same hash.
	hash1 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		// Simulate: PUSH1 0x02, PUSH1 0x03, ADD
		s.push(*uint256.NewInt(2))
		h.CaptureInputs(s, 0) // PUSH has 0 inputs
		h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)

		s.push(*uint256.NewInt(3))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 2, byte(PUSH1), 97, 3, s, 1)

		// ADD: pops 2, pushes 1
		h.CaptureInputs(s, 2) // capture top 2: [3, 2]
		x, y := s.pop(), s.peek()
		y.Add(&x, y) // 2 + 3 = 5
		h.HashOp(0, 4, byte(ADD), 94, 3, s, 1)
	})

	hash2 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(2))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)

		s.push(*uint256.NewInt(3))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 2, byte(PUSH1), 97, 3, s, 1)

		h.CaptureInputs(s, 2)
		x, y := s.pop(), s.peek()
		y.Add(&x, y)
		h.HashOp(0, 4, byte(ADD), 94, 3, s, 1)
	})

	require.Equal(t, hash1, hash2, "same operations must produce identical hashes")
}

func TestExecHasher_DifferentOps(t *testing.T) {
	// Different operations must produce different hashes.
	hashAdd := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(2))
		s.push(*uint256.NewInt(3))
		h.CaptureInputs(s, 2)
		x, y := s.pop(), s.peek()
		y.Add(&x, y)
		h.HashOp(0, 0, byte(ADD), 100, 3, s, 1)
	})

	hashSub := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(2))
		s.push(*uint256.NewInt(3))
		h.CaptureInputs(s, 2)
		x, y := s.pop(), s.peek()
		y.Sub(&x, y)
		h.HashOp(0, 0, byte(SUB), 100, 3, s, 1)
	})

	require.NotEqual(t, hashAdd, hashSub, "different opcodes must produce different hashes")
}

func TestExecHasher_DifferentInputs(t *testing.T) {
	// Same opcode but different stack values must produce different hashes.
	hash1 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(10))
		s.push(*uint256.NewInt(20))
		h.CaptureInputs(s, 2)
		x, y := s.pop(), s.peek()
		y.Add(&x, y)
		h.HashOp(0, 0, byte(ADD), 100, 3, s, 1)
	})

	hash2 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(30))
		s.push(*uint256.NewInt(40))
		h.CaptureInputs(s, 2)
		x, y := s.pop(), s.peek()
		y.Add(&x, y)
		h.HashOp(0, 0, byte(ADD), 100, 3, s, 1)
	})

	require.NotEqual(t, hash1, hash2, "different stack inputs must produce different hashes")
}

func TestExecHasher_DifferentGas(t *testing.T) {
	hash1 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(1))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 0, byte(PUSH1), 1000, 3, s, 1)
	})

	hash2 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(1))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 0, byte(PUSH1), 2000, 3, s, 1)
	})

	require.NotEqual(t, hash1, hash2, "different gas values must produce different hashes")
}

func TestExecHasher_DifferentDepth(t *testing.T) {
	hash1 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(1))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)
	})

	hash2 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(1))
		h.CaptureInputs(s, 0)
		h.HashOp(1, 0, byte(PUSH1), 100, 3, s, 1)
	})

	require.NotEqual(t, hash1, hash2, "different call depths must produce different hashes")
}

func TestExecHasher_EmptyTx(t *testing.T) {
	h := GetExecHasher()
	defer PutExecHasher(h)
	h.Reset()

	hash, ops := h.Finalize()
	require.Equal(t, uint64(0), ops)
	// Empty keccak256 should still produce a valid hash (not all zeros)
	require.NotEqual(t, [32]byte{}, hash)
}

func TestExecHasher_Precompile(t *testing.T) {
	h := GetExecHasher()
	defer PutExecHasher(h)
	h.Reset()

	addr := accounts.InternAddress(common.HexToAddress("0x01")) // ecrecover
	input := []byte{1, 2, 3, 4}
	output := []byte{5, 6, 7, 8}

	h.HashPrecompile(0, addr, input, output, 3000)
	hash1, ops := h.Finalize()
	require.Equal(t, uint64(1), ops)

	// Same precompile call should produce same hash
	h.Reset()
	h.HashPrecompile(0, addr, input, output, 3000)
	hash2, _ := h.Finalize()
	require.Equal(t, hash1, hash2)

	// Different input should produce different hash
	h.Reset()
	h.HashPrecompile(0, addr, []byte{9, 9, 9}, output, 3000)
	hash3, _ := h.Finalize()
	require.NotEqual(t, hash1, hash3)
}

func TestExecHasher_MixedOpsAndPrecompile(t *testing.T) {
	hash1 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		// Some EVM ops
		s.push(*uint256.NewInt(42))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)

		// Then a precompile call
		h.HashPrecompile(1, accounts.InternAddress(common.HexToAddress("0x02")), []byte{1}, []byte{2}, 500)

		// Then more EVM ops
		h.CaptureInputs(s, 1)
		_ = s.pop()
		h.HashOp(0, 5, byte(POP), 90, 2, s, 0)
	})

	hash2 := runHashSequence(t, func(h *ExecHasher, s *Stack) {
		s.push(*uint256.NewInt(42))
		h.CaptureInputs(s, 0)
		h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)

		h.HashPrecompile(1, accounts.InternAddress(common.HexToAddress("0x02")), []byte{1}, []byte{2}, 500)

		h.CaptureInputs(s, 1)
		_ = s.pop()
		h.HashOp(0, 5, byte(POP), 90, 2, s, 0)
	})

	require.Equal(t, hash1, hash2)
}

func TestExecHasher_Pool(t *testing.T) {
	h1 := GetExecHasher()
	h1.Reset()
	s := &Stack{data: make([]uint256.Int, 0, 16)}
	s.push(*uint256.NewInt(1))
	h1.CaptureInputs(s, 0)
	h1.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)
	expected, _ := h1.Finalize()
	PutExecHasher(h1)

	// Get from pool, reset, same ops should give same hash
	h2 := GetExecHasher()
	h2.Reset()
	s.Reset()
	s.push(*uint256.NewInt(1))
	h2.CaptureInputs(s, 0)
	h2.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)
	got, _ := h2.Finalize()
	PutExecHasher(h2)

	require.Equal(t, expected, got, "pooled hasher must produce same result after Reset")
}

func BenchmarkExecHasher_HashOp(b *testing.B) {
	h := GetExecHasher()
	defer PutExecHasher(h)
	h.Reset()

	s := &Stack{data: make([]uint256.Int, 0, 16)}
	s.push(*uint256.NewInt(0xdeadbeef))
	s.push(*uint256.NewInt(0xcafebabe))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate ADD: 2 inputs, 1 output
		h.CaptureInputs(s, 2)
		h.HashOp(0, uint64(i), byte(ADD), 100000, 3, s, 1)
	}
}

// runHashSequence runs a function that manipulates a stack and ExecHasher,
// and returns the finalized execution hash.
func runHashSequence(t *testing.T, fn func(h *ExecHasher, s *Stack)) [32]byte {
	t.Helper()
	h := GetExecHasher()
	defer PutExecHasher(h)
	h.Reset()

	s := &Stack{data: make([]uint256.Int, 0, 16)}
	fn(h, s)

	hash, _ := h.Finalize()
	return hash
}
