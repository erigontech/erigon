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

package vm

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func balAddr(hexByte byte) accounts.Address {
	var a common.Address
	a[19] = hexByte
	return accounts.InternAddress(a)
}

// findBalance / cacheBalance round-trip on a single frame, and a miss on a
// cold address.
func TestBalanceCacheFindMissAndHit(t *testing.T) {
	t.Parallel()
	cc := &CallContext{}
	addr := balAddr(0xaa)

	if _, ok := cc.findBalance(addr); ok {
		t.Fatal("findBalance hit on a cold cache")
	}

	want := *uint256.NewInt(12345)
	cc.cacheBalance(addr, want)

	got, ok := cc.findBalance(addr)
	if !ok {
		t.Fatal("findBalance miss after cacheBalance")
	}
	if !got.Eq(&want) {
		t.Fatalf("findBalance value: got %s want %s", got.String(), want.String())
	}
}

// findBalance walks the parent chain: a value cached on an ancestor is
// visible from a descendant frame whose own cache is empty.
func TestBalanceCacheParentChainWalk(t *testing.T) {
	t.Parallel()
	grandparent := &CallContext{}
	parent := &CallContext{parent: grandparent}
	child := &CallContext{parent: parent}

	addr := balAddr(0xbb)
	want := *uint256.NewInt(999)
	grandparent.cacheBalance(addr, want)

	got, ok := child.findBalance(addr)
	if !ok {
		t.Fatal("findBalance from child missed an ancestor-cached value")
	}
	if !got.Eq(&want) {
		t.Fatalf("findBalance value: got %s want %s", got.String(), want.String())
	}
}

// invalidateBalance deletes the entry from the calling frame AND every
// ancestor — the revert-safety primitive: after a balance mutation, no
// frame in the chain may serve a stale cached value.
func TestBalanceCacheInvalidateWalksChain(t *testing.T) {
	t.Parallel()
	grandparent := &CallContext{}
	parent := &CallContext{parent: grandparent}
	child := &CallContext{parent: parent}

	addr := balAddr(0xcc)
	val := *uint256.NewInt(500)
	grandparent.cacheBalance(addr, val)
	parent.cacheBalance(addr, val)
	child.cacheBalance(addr, val)

	child.invalidateBalance(addr)

	for name, cc := range map[string]*CallContext{
		"grandparent": grandparent,
		"parent":      parent,
		"child":       child,
	} {
		if _, ok := cc.balanceCache[addr]; ok {
			t.Fatalf("invalidateBalance left a stale entry on %s frame", name)
		}
	}
}

// A balance cached on the current frame shadows a stale value on an
// ancestor — findBalance walks current-frame-first, so a fresh local
// entry always wins over an out-of-date ancestor entry.
func TestBalanceCacheCurrentFrameShadowsAncestor(t *testing.T) {
	t.Parallel()
	parent := &CallContext{}
	child := &CallContext{parent: parent}

	addr := balAddr(0xdd)
	stale := *uint256.NewInt(100)
	fresh := *uint256.NewInt(150)
	parent.cacheBalance(addr, stale)
	child.cacheBalance(addr, fresh)

	got, ok := child.findBalance(addr)
	if !ok {
		t.Fatal("findBalance missed the current-frame entry")
	}
	if !got.Eq(&fresh) {
		t.Fatalf("findBalance returned the ancestor value: got %s want %s (fresh)", got.String(), fresh.String())
	}
}

// invalidateBalance on a cold address (no entry anywhere) is a safe no-op,
// and invalidating one address leaves others untouched.
func TestBalanceCacheInvalidateIsScopedAndSafe(t *testing.T) {
	t.Parallel()
	cc := &CallContext{}
	keep := balAddr(0x01)
	drop := balAddr(0x02)
	cold := balAddr(0x03)

	keepVal := *uint256.NewInt(7)
	cc.cacheBalance(keep, keepVal)
	cc.cacheBalance(drop, *uint256.NewInt(8))

	cc.invalidateBalance(cold) // no-op
	cc.invalidateBalance(drop)

	if _, ok := cc.findBalance(drop); ok {
		t.Fatal("invalidateBalance failed to remove the target entry")
	}
	got, ok := cc.findBalance(keep)
	if !ok || !got.Eq(&keepVal) {
		t.Fatal("invalidateBalance disturbed an unrelated entry")
	}
}
