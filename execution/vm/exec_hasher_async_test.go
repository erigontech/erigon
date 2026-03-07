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

// TestAsyncExecHasher_MatchesSync verifies that the async hasher produces
// the exact same hash as the synchronous ExecHasher for identical operations.
func TestAsyncExecHasher_MatchesSync(t *testing.T) {
	// Run with sync hasher
	syncH := GetExecHasher()
	syncH.Reset()
	s1 := &Stack{data: make([]uint256.Int, 0, 16)}

	s1.push(*uint256.NewInt(2))
	syncH.CaptureInputs(s1, 0)
	syncH.HashOp(0, 0, byte(PUSH1), 100, 3, s1, 1)

	s1.push(*uint256.NewInt(3))
	syncH.CaptureInputs(s1, 0)
	syncH.HashOp(0, 2, byte(PUSH1), 97, 3, s1, 1)

	syncH.CaptureInputs(s1, 2)
	x, y := s1.pop(), s1.peek()
	y.Add(&x, y)
	syncH.HashOp(0, 4, byte(ADD), 94, 3, s1, 1)

	syncHash, syncOps := syncH.Finalize()
	PutExecHasher(syncH)

	// Run with async hasher
	asyncH := GetAsyncExecHasher()
	asyncH.Start()
	s2 := &Stack{data: make([]uint256.Int, 0, 16)}

	s2.push(*uint256.NewInt(2))
	asyncH.CaptureInputs(s2, 0)
	asyncH.HashOp(0, 0, byte(PUSH1), 100, 3, s2, 1)

	s2.push(*uint256.NewInt(3))
	asyncH.CaptureInputs(s2, 0)
	asyncH.HashOp(0, 2, byte(PUSH1), 97, 3, s2, 1)

	asyncH.CaptureInputs(s2, 2)
	x2, y2 := s2.pop(), s2.peek()
	y2.Add(&x2, y2)
	asyncH.HashOp(0, 4, byte(ADD), 94, 3, s2, 1)

	asyncHash, asyncOps := asyncH.Finalize()
	PutAsyncExecHasher(asyncH)

	require.Equal(t, syncHash, asyncHash, "async and sync hashers must produce identical hashes")
	require.Equal(t, syncOps, asyncOps)
}

// TestAsyncExecHasher_ManyOps pushes many operations to stress the ring buffer.
func TestAsyncExecHasher_ManyOps(t *testing.T) {
	h := GetAsyncExecHasher()
	h.Start()

	s := &Stack{data: make([]uint256.Int, 0, 16)}
	s.push(*uint256.NewInt(0xdeadbeef))
	s.push(*uint256.NewInt(0xcafebabe))

	const numOps = 10_000
	for i := 0; i < numOps; i++ {
		h.CaptureInputs(s, 2)
		h.HashOp(0, uint64(i), byte(ADD), 100000, 3, s, 1)
	}

	hash, ops := h.Finalize()
	PutAsyncExecHasher(h)

	require.Equal(t, uint64(numOps), ops)
	require.NotEqual(t, [32]byte{}, hash)

	// Verify determinism: do it again
	h2 := GetAsyncExecHasher()
	h2.Start()

	s2 := &Stack{data: make([]uint256.Int, 0, 16)}
	s2.push(*uint256.NewInt(0xdeadbeef))
	s2.push(*uint256.NewInt(0xcafebabe))

	for i := 0; i < numOps; i++ {
		h2.CaptureInputs(s2, 2)
		h2.HashOp(0, uint64(i), byte(ADD), 100000, 3, s2, 1)
	}

	hash2, _ := h2.Finalize()
	PutAsyncExecHasher(h2)

	require.Equal(t, hash, hash2, "async hasher must be deterministic")
}

// TestAsyncExecHasher_Precompile tests synthetic precompile records.
func TestAsyncExecHasher_Precompile(t *testing.T) {
	// Sync
	syncH := GetExecHasher()
	syncH.Reset()
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	syncH.HashPrecompile(0, addr, []byte{1, 2, 3}, []byte{4, 5, 6}, 3000)
	syncHash, _ := syncH.Finalize()
	PutExecHasher(syncH)

	// Async
	asyncH := GetAsyncExecHasher()
	asyncH.Start()
	asyncH.HashPrecompile(0, addr, []byte{1, 2, 3}, []byte{4, 5, 6}, 3000)
	asyncHash, _ := asyncH.Finalize()
	PutAsyncExecHasher(asyncH)

	require.Equal(t, syncHash, asyncHash, "precompile hash must match between sync and async")
}

// TestAsyncExecHasher_Empty tests finalizing with no operations.
func TestAsyncExecHasher_Empty(t *testing.T) {
	// Sync empty
	syncH := GetExecHasher()
	syncH.Reset()
	syncHash, _ := syncH.Finalize()
	PutExecHasher(syncH)

	// Async empty
	asyncH := GetAsyncExecHasher()
	asyncH.Start()
	asyncHash, ops := asyncH.Finalize()
	PutAsyncExecHasher(asyncH)

	require.Equal(t, syncHash, asyncHash, "empty hash must match")
	require.Equal(t, uint64(0), ops)
}

// TestAsyncExecHasher_Reuse tests that the hasher can be reused after Finalize.
func TestAsyncExecHasher_Reuse(t *testing.T) {
	h := GetAsyncExecHasher()

	// First use
	h.Start()
	s := &Stack{data: make([]uint256.Int, 0, 16)}
	s.push(*uint256.NewInt(42))
	h.CaptureInputs(s, 0)
	h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)
	hash1, _ := h.Finalize()

	// Second use - same ops should give same hash
	h.Start()
	s.Reset()
	s.push(*uint256.NewInt(42))
	h.CaptureInputs(s, 0)
	h.HashOp(0, 0, byte(PUSH1), 100, 3, s, 1)
	hash2, _ := h.Finalize()

	PutAsyncExecHasher(h)

	require.Equal(t, hash1, hash2, "reused async hasher must produce same result")
}

func BenchmarkAsyncExecHasher_HashOp(b *testing.B) {
	h := GetAsyncExecHasher()
	h.Start()

	s := &Stack{data: make([]uint256.Int, 0, 16)}
	s.push(*uint256.NewInt(0xdeadbeef))
	s.push(*uint256.NewInt(0xcafebabe))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.CaptureInputs(s, 2)
		h.HashOp(0, uint64(i), byte(ADD), 100000, 3, s, 1)
	}
	b.StopTimer()

	h.Finalize()
	PutAsyncExecHasher(h)
}
