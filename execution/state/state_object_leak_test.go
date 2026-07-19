// Copyright 2026 The Erigon Authors
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

package state

import (
	"runtime"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func enableAssert(t *testing.T) {
	t.Helper()
	prev := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = prev })
}

// waitForLeakReports drives GC until the leak counter moves past want-1, since
// finalizers run asynchronously after collection.
func waitForLeakReports(base int64) int64 {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if got := leakedStateObjects.Load(); got > base {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return leakedStateObjects.Load()
}

func TestLeakedStateObjectIsReported(t *testing.T) {
	enableAssert(t)

	base := leakedStateObjects.Load()
	func() { _ = getStateObject() }() // dropped without release

	if got := waitForLeakReports(base); got <= base {
		t.Fatalf("leaked stateObject was not reported: counter stayed at %d", base)
	}
}

func TestReleasedStateObjectIsNotReported(t *testing.T) {
	enableAssert(t)

	base := leakedStateObjects.Load()
	func() {
		so := getStateObject()
		so.db = nil
		so.release()
	}()

	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if got := leakedStateObjects.Load(); got != base {
		t.Fatalf("released stateObject was reported as leaked: %d -> %d", base, got)
	}
}

func waitForWriteSetLeaks(base int64) int64 {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if got := leakedWriteSets.Load(); got > base {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return leakedWriteSets.Load()
}

func TestDroppedWriteSetIsReported(t *testing.T) {
	enableAssert(t)

	base := leakedWriteSets.Load()
	func() {
		ws := NewWriteSet()
		ws.SetBalance(accounts.NilAddress, &VersionedWrite[uint256.Int]{})
	}() // dropped without ReleaseAndReset, holding a pooled map

	if got := waitForWriteSetLeaks(base); got <= base {
		t.Fatalf("dropped WriteSet was not reported: counter stayed at %d", base)
	}
}

func TestReleasedWriteSetIsNotReported(t *testing.T) {
	enableAssert(t)

	base := leakedWriteSets.Load()
	func() {
		ws := NewWriteSet()
		ws.SetBalance(accounts.NilAddress, &VersionedWrite[uint256.Int]{})
		ws.ReleaseAndReset()
	}()

	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if got := leakedWriteSets.Load(); got != base {
		t.Fatalf("released WriteSet was reported as leaked: %d -> %d", base, got)
	}
}

// An empty WriteSet strands nothing, so dropping one must stay silent -
// otherwise the common &WriteSet{} that never checks out a map would drown
// the real reports.
func TestEmptyWriteSetIsNotReported(t *testing.T) {
	enableAssert(t)

	base := leakedWriteSets.Load()
	func() { _ = NewWriteSet() }()

	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if got := leakedWriteSets.Load(); got != base {
		t.Fatalf("empty WriteSet was reported as leaked: %d -> %d", base, got)
	}
}
