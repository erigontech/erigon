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
	"encoding/binary"
	"sync"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// ExecHasher computes a rolling keccak256 hash over every EVM instruction
// executed during a transaction. It captures spec-level fields (depth, pc,
// opcode, gas, cost, stack inputs/outputs) to produce a deterministic
// execution fingerprint.
//
// Usage:
//
//	h := GetExecHasher()
//	defer PutExecHasher(h)
//	h.Reset()
//	// ... set on EVM, execute tx ...
//	hash := h.Finalize()
type ExecHasher struct {
	hasher keccak.KeccakState
	buf    [27]byte   // fixed-size header: depth(2) + pc(8) + op(1) + gas(8) + cost(8)
	inBuf  [16][32]byte // saved stack inputs (max 16 items, each 32 bytes)
	inN    int           // number of saved input items
	ops    uint64        // opcode counter for diagnostics
}

var execHasherPool = sync.Pool{
	New: func() any {
		return &ExecHasher{}
	},
}

// GetExecHasher returns an ExecHasher from the pool.
func GetExecHasher() *ExecHasher {
	return execHasherPool.Get().(*ExecHasher)
}

// PutExecHasher returns an ExecHasher to the pool.
func PutExecHasher(h *ExecHasher) {
	execHasherPool.Put(h)
}

// Reset prepares the hasher for a new transaction.
func (h *ExecHasher) Reset() {
	if h.hasher == nil {
		h.hasher = crypto.NewKeccakState()
	} else {
		h.hasher.Reset()
	}
	h.inN = 0
	h.ops = 0
}

// CaptureInputs snapshots the top numPop stack items BEFORE the opcode executes.
// These items will be consumed by the operation.
func (h *ExecHasher) CaptureInputs(stack *Stack, numPop int) {
	if numPop == 0 {
		h.inN = 0
		return
	}
	n := numPop
	if n > 16 {
		n = 16
	}
	sLen := stack.len()
	for i := 0; i < n; i++ {
		item := &stack.data[sLen-1-i]
		item.WriteToSlice(h.inBuf[i][:])
	}
	h.inN = n
}

// HashOp encodes and hashes one instruction record AFTER execution.
// The stack now contains the outputs; inputs were captured by CaptureInputs.
func (h *ExecHasher) HashOp(depth int, pc uint64, op byte, gas, cost uint64, stack *Stack, numPush int) {
	// Encode fixed header
	binary.BigEndian.PutUint16(h.buf[0:2], uint16(depth))
	binary.BigEndian.PutUint64(h.buf[2:10], pc)
	h.buf[10] = op
	binary.BigEndian.PutUint64(h.buf[11:19], gas)
	binary.BigEndian.PutUint64(h.buf[19:27], cost)
	h.hasher.Write(h.buf[:])

	// Write saved inputs
	for i := 0; i < h.inN; i++ {
		h.hasher.Write(h.inBuf[i][:])
	}

	// Write outputs (top numPush items on the stack after execute)
	if numPush > 0 {
		sLen := stack.len()
		var outBuf [32]byte
		for i := 0; i < numPush && i < sLen; i++ {
			stack.data[sLen-1-i].WriteToSlice(outBuf[:])
			h.hasher.Write(outBuf[:])
		}
	}

	h.ops++
}

// HashPrecompile hashes a synthetic record for a precompile call.
// Precompiles bypass the EVM loop, so we emit a special record.
func (h *ExecHasher) HashPrecompile(depth int, addr accounts.Address, input, output []byte, gasUsed uint64) {
	// Header: depth(2) + sentinel_op(1) = 0xFE (INVALID opcode as discriminator)
	binary.BigEndian.PutUint16(h.buf[0:2], uint16(depth))
	h.buf[2] = 0xFE // sentinel: precompile marker
	binary.BigEndian.PutUint64(h.buf[3:11], gasUsed)
	h.hasher.Write(h.buf[:11])

	// Precompile address (raw 20 bytes)
	addrVal := addr.Value()
	h.hasher.Write(addrVal[:])

	// Hash of input
	inputHasher := crypto.NewKeccakState()
	inputHasher.Write(input)
	var inputHash [32]byte
	inputHasher.Read(inputHash[:])
	crypto.ReturnToPool(inputHasher)
	h.hasher.Write(inputHash[:])

	// Hash of output
	outputHasher := crypto.NewKeccakState()
	outputHasher.Write(output)
	var outputHash [32]byte
	outputHasher.Read(outputHash[:])
	crypto.ReturnToPool(outputHasher)
	h.hasher.Write(outputHash[:])

	h.ops++
}

// Finalize returns the execution hash and the number of opcodes hashed.
func (h *ExecHasher) Finalize() (hash [32]byte, ops uint64) {
	h.hasher.Read(hash[:])
	return hash, h.ops
}

// Ops returns the number of opcodes hashed so far.
func (h *ExecHasher) Ops() uint64 {
	return h.ops
}
