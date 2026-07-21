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

package cachebudget

import "testing"

func TestReserveStopsAtLimit(t *testing.T) {
	b := New(1000)
	if !b.Reserve(600) {
		t.Fatal("first Reserve(600) should fit")
	}
	if !b.Reserve(400) {
		t.Fatal("second Reserve(400) should exactly fill")
	}
	if b.Reserve(1) {
		t.Fatal("Reserve past the limit must fail and take nothing")
	}
	if b.Used() != 1000 {
		t.Fatalf("used: got %d want 1000", b.Used())
	}
}

func TestReleaseReopensRoom(t *testing.T) {
	b := New(1000)
	b.Reserve(1000)
	b.Release(400)
	if !b.Reserve(400) {
		t.Fatal("after Release(400) a Reserve(400) should fit")
	}
	if b.Reserve(1) {
		t.Fatal("still full after regrow")
	}
}

func TestTakeIsUnconditional(t *testing.T) {
	b := New(100)
	b.Take(500) // initial small allocation always succeeds even past limit
	if b.Used() != 500 {
		t.Fatalf("used: got %d want 500", b.Used())
	}
	if b.Reserve(1) {
		t.Fatal("over-committed envelope refuses further Reserve")
	}
}

func TestGlobalSizedFromMemory(t *testing.T) {
	if Global.Limit() <= 0 {
		t.Fatalf("Global envelope must be positive, got %d", Global.Limit())
	}
}
