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
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// CallFrame holds state for a single execution context in the iterative EVM.
// It captures all state needed to pause execution for a nested call and resume afterward.
type CallFrame struct {
	// Execution state
	callContext *CallContext // Stack, Memory, gas, Contract
	pc          uint64       // Program counter
	op          OpCode       // Current opcode being executed

	// Call metadata
	callType OpCode // CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2
	readOnly bool   // Whether this frame is in readOnly mode
	depth    int    // Call depth for this frame

	// State management
	snapshot int // State snapshot ID for reversion

	// For resuming after child call returns (CALL/CALLCODE/DELEGATECALL/STATICCALL)
	retOffset uint64 // Memory offset for return data
	retSize   uint64 // Size of return buffer

	// For CREATE/CREATE2 - the address being created
	createAddr accounts.Address

	// Return values from child call (set when child completes)
	pendingRet []byte // Return data from child
	pendingGas uint64 // Leftover gas from child
	pendingErr error  // Error from child (nil = success)

	// Indicates we're waiting for a child call to complete
	hasPendingCall bool
}

// Reset clears the frame for reuse from the pool
func (f *CallFrame) Reset() {
	f.callContext = nil
	f.pc = 0
	f.op = 0
	f.callType = 0
	f.readOnly = false
	f.depth = 0
	f.snapshot = 0
	f.retOffset = 0
	f.retSize = 0
	f.createAddr = accounts.Address{}
	f.pendingRet = nil
	f.pendingGas = 0
	f.pendingErr = nil
	f.hasPendingCall = false
}

// framePool provides CallFrame reuse to reduce allocations
var framePool = sync.Pool{
	New: func() any {
		return &CallFrame{}
	},
}

// getFrame retrieves a CallFrame from the pool
func getFrame() *CallFrame {
	return framePool.Get().(*CallFrame)
}

// putFrame returns a CallFrame to the pool after resetting it
func putFrame(f *CallFrame) {
	f.Reset()
	framePool.Put(f)
}

// PendingCall contains information needed to set up a child call frame.
// This is populated by call opcodes and consumed by the main loop.
type PendingCall struct {
	CallType   OpCode           // CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2
	Caller     accounts.Address // Who is making the call
	CallerAddr accounts.Address // For DELEGATECALL: the original caller address
	Addr       accounts.Address // Target address
	Input      []byte           // Call input data
	Gas        uint64           // Gas to provide to the call
	Value      uint256.Int      // Value to transfer (CALL, CALLCODE, CREATE, CREATE2)
	Salt       uint256.Int      // Salt for CREATE2
	RetOffset  uint64           // Memory offset for return data (calls only)
	RetSize    uint64           // Size of return buffer (calls only)
	IsReadOnly bool             // Whether the call should be read-only
	IsCreate   bool             // True for CREATE/CREATE2
	IsCreate2  bool             // True specifically for CREATE2
}

// CallStack manages explicit call frames for iterative EVM execution.
// It replaces the implicit Go call stack used in recursive execution.
type CallStack struct {
	frames  []*CallFrame
	maxSize int
}

// NewCallStack creates a new CallStack with the EVM depth limit.
func NewCallStack() *CallStack {
	return &CallStack{
		frames:  make([]*CallFrame, 0, 64), // Pre-allocate reasonable capacity
		maxSize: 1024,                      // params.CallCreateDepth
	}
}

// Push adds a frame to the stack. Returns ErrDepth if at maximum depth.
func (cs *CallStack) Push(frame *CallFrame) error {
	if len(cs.frames) >= cs.maxSize {
		return ErrDepth
	}
	cs.frames = append(cs.frames, frame)
	return nil
}

// Pop removes and returns the top frame. Returns nil if stack is empty.
func (cs *CallStack) Pop() *CallFrame {
	if len(cs.frames) == 0 {
		return nil
	}
	n := len(cs.frames) - 1
	frame := cs.frames[n]
	cs.frames[n] = nil // Allow GC
	cs.frames = cs.frames[:n]
	return frame
}

// Peek returns the top frame without removing it. Returns nil if empty.
func (cs *CallStack) Peek() *CallFrame {
	if len(cs.frames) == 0 {
		return nil
	}
	return cs.frames[len(cs.frames)-1]
}

// Depth returns the current stack depth.
func (cs *CallStack) Depth() int {
	return len(cs.frames)
}

// IsEmpty returns true if the stack has no frames.
func (cs *CallStack) IsEmpty() bool {
	return len(cs.frames) == 0
}

// Clear removes all frames from the stack and returns them to the pool.
func (cs *CallStack) Clear() {
	for i, frame := range cs.frames {
		if frame != nil {
			// Return CallContext to its pool if present
			if frame.callContext != nil {
				frame.callContext.put()
			}
			putFrame(frame)
		}
		cs.frames[i] = nil
	}
	cs.frames = cs.frames[:0]
}
