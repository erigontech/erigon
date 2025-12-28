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
	"testing"
)

func TestCallStack_PushPop(t *testing.T) {
	cs := NewCallStack()

	if !cs.IsEmpty() {
		t.Error("new CallStack should be empty")
	}
	if cs.Depth() != 0 {
		t.Errorf("new CallStack depth should be 0, got %d", cs.Depth())
	}

	// Push a frame
	frame1 := getFrame()
	frame1.pc = 100
	if err := cs.Push(frame1); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	if cs.IsEmpty() {
		t.Error("CallStack should not be empty after Push")
	}
	if cs.Depth() != 1 {
		t.Errorf("CallStack depth should be 1, got %d", cs.Depth())
	}

	// Peek should return the frame without removing it
	peeked := cs.Peek()
	if peeked != frame1 {
		t.Error("Peek should return the same frame")
	}
	if peeked.pc != 100 {
		t.Errorf("Peeked frame pc should be 100, got %d", peeked.pc)
	}
	if cs.Depth() != 1 {
		t.Error("Peek should not change depth")
	}

	// Push another frame
	frame2 := getFrame()
	frame2.pc = 200
	if err := cs.Push(frame2); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if cs.Depth() != 2 {
		t.Errorf("CallStack depth should be 2, got %d", cs.Depth())
	}

	// Peek should return frame2
	peeked = cs.Peek()
	if peeked != frame2 {
		t.Error("Peek should return the top frame")
	}

	// Pop should return frame2
	popped := cs.Pop()
	if popped != frame2 {
		t.Error("Pop should return the top frame")
	}
	if cs.Depth() != 1 {
		t.Errorf("CallStack depth should be 1 after Pop, got %d", cs.Depth())
	}

	// Pop should return frame1
	popped = cs.Pop()
	if popped != frame1 {
		t.Error("Pop should return frame1")
	}
	if !cs.IsEmpty() {
		t.Error("CallStack should be empty after all pops")
	}

	// Pop on empty stack should return nil
	popped = cs.Pop()
	if popped != nil {
		t.Error("Pop on empty stack should return nil")
	}

	// Peek on empty stack should return nil
	peeked = cs.Peek()
	if peeked != nil {
		t.Error("Peek on empty stack should return nil")
	}

	// Return frames to pool
	putFrame(frame1)
	putFrame(frame2)
}

func TestCallStack_DepthLimit(t *testing.T) {
	cs := NewCallStack()

	// Push up to the limit (1024)
	frames := make([]*CallFrame, 1024)
	for i := 0; i < 1024; i++ {
		frames[i] = getFrame()
		frames[i].pc = uint64(i)
		if err := cs.Push(frames[i]); err != nil {
			t.Fatalf("Push at depth %d failed: %v", i, err)
		}
	}

	if cs.Depth() != 1024 {
		t.Errorf("CallStack depth should be 1024, got %d", cs.Depth())
	}

	// Next push should fail with ErrDepth
	extraFrame := getFrame()
	err := cs.Push(extraFrame)
	if err != ErrDepth {
		t.Errorf("Push beyond limit should return ErrDepth, got %v", err)
	}
	putFrame(extraFrame)

	// Clean up
	cs.Clear()
}

func TestCallStack_Clear(t *testing.T) {
	cs := NewCallStack()

	// Push some frames
	for i := 0; i < 5; i++ {
		frame := getFrame()
		frame.pc = uint64(i)
		if err := cs.Push(frame); err != nil {
			t.Fatalf("Push failed: %v", err)
		}
	}

	if cs.Depth() != 5 {
		t.Errorf("CallStack depth should be 5, got %d", cs.Depth())
	}

	// Clear should empty the stack
	cs.Clear()

	if !cs.IsEmpty() {
		t.Error("CallStack should be empty after Clear")
	}
	if cs.Depth() != 0 {
		t.Errorf("CallStack depth should be 0 after Clear, got %d", cs.Depth())
	}
}

func TestCallFrame_Reset(t *testing.T) {
	frame := getFrame()

	// Set various fields
	frame.pc = 12345
	frame.op = CALL
	frame.callType = DELEGATECALL
	frame.readOnly = true
	frame.depth = 10
	frame.snapshot = 5
	frame.retOffset = 100
	frame.retSize = 200
	frame.pendingRet = []byte{1, 2, 3}
	frame.pendingGas = 1000
	frame.pendingErr = ErrOutOfGas
	frame.hasPendingCall = true

	// Reset should clear all fields
	frame.Reset()

	if frame.pc != 0 {
		t.Error("Reset should clear pc")
	}
	if frame.op != 0 {
		t.Error("Reset should clear op")
	}
	if frame.callType != 0 {
		t.Error("Reset should clear callType")
	}
	if frame.readOnly {
		t.Error("Reset should clear readOnly")
	}
	if frame.depth != 0 {
		t.Error("Reset should clear depth")
	}
	if frame.snapshot != 0 {
		t.Error("Reset should clear snapshot")
	}
	if frame.retOffset != 0 {
		t.Error("Reset should clear retOffset")
	}
	if frame.retSize != 0 {
		t.Error("Reset should clear retSize")
	}
	if frame.pendingRet != nil {
		t.Error("Reset should clear pendingRet")
	}
	if frame.pendingGas != 0 {
		t.Error("Reset should clear pendingGas")
	}
	if frame.pendingErr != nil {
		t.Error("Reset should clear pendingErr")
	}
	if frame.hasPendingCall {
		t.Error("Reset should clear hasPendingCall")
	}

	putFrame(frame)
}

func TestFramePool(t *testing.T) {
	// Get a frame from pool
	frame1 := getFrame()
	if frame1 == nil {
		t.Fatal("getFrame should not return nil")
	}

	// Set some values
	frame1.pc = 999

	// Return to pool
	putFrame(frame1)

	// Get another frame - may or may not be the same one
	frame2 := getFrame()
	if frame2 == nil {
		t.Fatal("getFrame should not return nil")
	}

	// Frame should be reset
	if frame2.pc != 0 {
		t.Error("Frame from pool should be reset")
	}

	putFrame(frame2)
}

func BenchmarkCallStack_PushPop(b *testing.B) {
	cs := NewCallStack()
	frame := getFrame()
	defer putFrame(frame)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs.Push(frame)
		cs.Pop()
	}
}

func BenchmarkFramePool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame := getFrame()
		putFrame(frame)
	}
}
