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

	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// CallFrame holds state for a single execution context in the iterative EVM.
// It captures all state needed to pause execution for a nested call and resume afterward.
type CallFrame struct {
	// Execution state
	callContext *CallContext // Stack, Memory, gas
	contract    Contract     // Contract being executed (stored directly for fast access)
	pc          uint64       // Program counter
	op          OpCode       // Current opcode being executed

	// Call metadata
	callType OpCode // CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2
	readOnly bool   // Whether this frame is in readOnly mode

	// State management
	snapshot int // State snapshot ID for reversion

	// For resuming after child call returns (CALL/CALLCODE/DELEGATECALL/STATICCALL)
	retOffset uint64 // Memory offset for return data
	retSize   uint64 // Size of return buffer

	// For CREATE/CREATE2 - the address being created
	createAddr accounts.Address
	isCreate   bool // True if this frame is executing CREATE/CREATE2 init code

	// Tracer support
	startGas uint64           // Initial gas for this frame (for tracer OnExit calculation)
	caller   accounts.Address // Caller address for tracer
	target   accounts.Address // Target/to address for tracer
	value    uint256.Int      // Value transferred for tracer
}

// Reset clears the frame for reuse from the pool
func (f *CallFrame) Reset() {
	f.callContext = nil
	f.contract = Contract{}
	f.pc = 0
	f.op = 0
	f.callType = 0
	f.readOnly = false
	f.snapshot = 0
	f.retOffset = 0
	f.retSize = 0
	f.createAddr = accounts.Address{}
	f.isCreate = false
	f.startGas = 0
	f.caller = accounts.Address{}
	f.target = accounts.Address{}
	f.value = uint256.Int{}
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

// Reset clears the PendingCall for reuse
func (p *PendingCall) Reset() {
	p.CallType = 0
	p.Caller = accounts.Address{}
	p.CallerAddr = accounts.Address{}
	p.Addr = accounts.Address{}
	p.Input = nil
	p.Gas = 0
	p.Value = uint256.Int{}
	p.Salt = uint256.Int{}
	p.RetOffset = 0
	p.RetSize = 0
	p.IsReadOnly = false
	p.IsCreate = false
	p.IsCreate2 = false
}

// pendingCallPool provides PendingCall reuse to reduce allocations
var pendingCallPool = sync.Pool{
	New: func() any {
		return &PendingCall{}
	},
}

// getPendingCall retrieves a PendingCall from the pool
func getPendingCall() *PendingCall {
	return pendingCallPool.Get().(*PendingCall)
}

// putPendingCall returns a PendingCall to the pool after resetting it
func putPendingCall(p *PendingCall) {
	p.Reset()
	pendingCallPool.Put(p)
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
		frames:  make([]*CallFrame, 0, 64),  // Pre-allocate reasonable capacity
		maxSize: int(params.CallCreateDepth), // EVM max call depth
	}
}

// Push adds a frame to the stack. Returns ErrDepth if at maximum depth.
// We allow up to maxSize+1 frames because the depth check in PrepareCall
// needs to run at depth maxSize+1 to fail properly.
func (cs *CallStack) Push(frame *CallFrame) error {
	if len(cs.frames) > cs.maxSize {
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
