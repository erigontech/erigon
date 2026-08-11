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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// staticAccountReader returns a fixed account without allocating, so an
// AllocsPerRun loop measures only the IntraBlockState path.
type staticAccountReader struct {
	emptyReader
	addr accounts.Address
	acc  *accounts.Account
}

func (r *staticAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr {
		return r.acc, nil
	}
	return nil, nil
}

// anyAccountReader answers every address with the same account, so a benchmark
// can touch an arbitrary number of distinct accounts.
type anyAccountReader struct {
	emptyReader
	acc *accounts.Account
}

func (r *anyAccountReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return r.acc, nil
}

func newNoMaterializeIBS(reader StateReader) (*IntraBlockState, *VersionMap) {
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	return ibs, vm
}

func startNoMaterializeTx(ibs *IntraBlockState, vm *VersionMap, txIndex int) {
	ibs.Reset()
	ibs.SetVersionMap(vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTransientObjectArena(true)
	ibs.SetTxContext(1, txIndex)
}

// TestNoMaterializeWithoutRewindSkipsArena pins the opt-in: a caller that never
// calls Reset (the BAL block assembler) must not draw arena slots, which it would
// otherwise hold — with the account and code they last served — for the whole
// block. Reset clears the opt-in, so only a caller that re-enables it after every
// rewind can reach the arena.
func TestNoMaterializeWithoutRewindSkipsArena(t *testing.T) {
	ibs, _ := newNoMaterializeIBS(&emptyReader{})
	defer ibs.Close()
	ibs.SetNoMaterialize(true) // no SetTransientObjectArena: this caller never rewinds

	so := ibs.allocStateObject()
	assert.False(t, so.arena, "a caller that never rewinds must not draw from the arena")
	assert.Empty(t, ibs.stateObjectArena.slabs, "arena must stay unallocated")
}

// TestResetClearsArenaOptIn pins that the opt-in does not survive a rewind: a
// reused IntraBlockState that forgets to re-enable it falls back to the heap
// rather than silently parking objects.
func TestResetClearsArenaOptIn(t *testing.T) {
	ibs, _ := newNoMaterializeIBS(&emptyReader{})
	defer ibs.Close()

	ibs.SetNoMaterialize(true)
	ibs.SetTransientObjectArena(true)
	require.True(t, ibs.allocStateObject().arena)

	ibs.Reset()
	ibs.SetNoMaterialize(true)
	assert.False(t, ibs.allocStateObject().arena, "Reset must clear the arena opt-in")
}

// TestNoMaterializeReadReusesArena pins that account reads on the parallel path
// are served from the arena and that consecutive transactions reuse the same
// slab instead of growing it.
func TestNoMaterializeReadReusesArena(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xAB})
	acc := accounts.NewAccount()
	acc.Nonce = 3
	acc.Balance.SetUint64(77)

	ibs, vm := newNoMaterializeIBS(&staticAccountReader{addr: addr, acc: &acc})
	defer ibs.Close()

	slabsAfterFirstTx := 0
	for txIndex := range 200 {
		startNoMaterializeTx(ibs, vm, txIndex)
		balance, err := ibs.GetBalance(addr)
		require.NoError(t, err)
		require.EqualValues(t, 77, balance.Uint64())
		require.Empty(t, ibs.stateObjects, "parallel read must not cache a stateObject")
		if txIndex == 0 {
			slabsAfterFirstTx = len(ibs.stateObjectArena.slabs)
			require.NotZero(t, slabsAfterFirstTx, "the read must be served from the arena")
		}
	}

	require.Equal(t, slabsAfterFirstTx, len(ibs.stateObjectArena.slabs),
		"the arena must stop growing after the first transaction")
}

// TestNoMaterializeAllocStateObjectUsesArena pins the routing: the parallel path
// takes arena slots, the materializing path takes pooled objects.
func TestNoMaterializeAllocStateObjectUsesArena(t *testing.T) {
	ibs, _ := newNoMaterializeIBS(&emptyReader{})
	defer ibs.Close()

	pooled := ibs.allocStateObject()
	assert.False(t, pooled.arena, "materializing path must use the pool")
	pooled.release()

	ibs.SetNoMaterialize(true)
	ibs.SetTransientObjectArena(true)
	assert.True(t, ibs.allocStateObject().arena, "parallel path must use the arena")
}

// TestNoMaterializeOverflowSkipsPool pins that once the arena is full the
// parallel path allocates rather than draining the shared pool, since objects it
// hands out are never released back.
func TestNoMaterializeOverflowSkipsPool(t *testing.T) {
	ibs, _ := newNoMaterializeIBS(&emptyReader{})
	defer ibs.Close()
	ibs.SetNoMaterialize(true)
	ibs.SetTransientObjectArena(true)

	for range arenaMaxObjects {
		require.True(t, ibs.allocStateObject().arena)
	}

	// Only a pool draw can hand a sentinel back. The reverse does not hold: a GC
	// or a P migration between Put and Get empties the pool, so a drain can slip
	// through - but a correct alloc can never fail this.
	sentinels := make([]*stateObject, 4)
	for i := range sentinels {
		sentinels[i] = newHeapObject()
		stateObjectPool.Put(sentinels[i])
	}
	// stateObjectPool is package-global: drain what this test pushed so sibling
	// tests do not run against a pool it primed.
	t.Cleanup(func() {
		for range sentinels {
			stateObjectPool.Get()
		}
	})

	overflow := ibs.allocStateObject()
	require.False(t, overflow.arena, "overflow object is not an arena slot")
	for i, sentinel := range sentinels {
		require.NotSame(t, sentinel, overflow, "overflow drew sentinel %d from the shared pool", i)
	}
	overflow.setState(accounts.InternKey([32]byte{0x01}), *uint256.NewInt(1))
	require.Len(t, overflow.dirtyStorage, 1, "overflow object must be usable")
}

// TestNoMaterializeAccountReadAllocs guards against the stateObject allocation
// coming back. The residual allocations belong to the per-transaction ReadSet,
// not to the stateObject.
func TestNoMaterializeAccountReadAllocs(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xAB})
	acc := accounts.NewAccount()
	acc.Nonce = 3
	acc.Balance.SetUint64(77)

	ibs, vm := newNoMaterializeIBS(&staticAccountReader{addr: addr, acc: &acc})
	defer ibs.Close()

	allocs := testing.AllocsPerRun(200, func() {
		startNoMaterializeTx(ibs, vm, 0)
		if _, err := ibs.GetBalance(addr); err != nil {
			t.Fatal(err)
		}
	})

	// Assert the arena served the read directly: the alloc bound alone would still
	// pass if allocStateObject regressed to a pool draw, since a warm pool also
	// costs nothing.
	require.NotZero(t, ibs.stateObjectArena.idx, "the read must be served from the arena")

	//goland:noinspection GoBoolExpressions
	if !race.Enabled {
		require.LessOrEqual(t, allocs, 5.0, "stateObject must not be heap-allocated per transaction")
	}
}

// TestStateObjectArenaRecyclesSlots pins that reset hands the same backing
// slots out again, carrying no state from their previous use.
func TestStateObjectArenaRecyclesSlots(t *testing.T) {
	var a stateObjectArena

	first := a.alloc()
	require.NotNil(t, first)
	first.selfdestructed = true
	first.deleted = true
	first.dirtyCode = true
	first.data.Nonce = 9
	first.code = accounts.Code{Bytes: []byte{0x60}}
	first.setState(accounts.InternKey([32]byte{0x01}), *uint256.NewInt(5))

	second := a.alloc()
	require.NotNil(t, second)
	require.NotSame(t, first, second, "live slots must not alias")

	a.reset()

	reused := a.alloc()
	require.Same(t, first, reused, "reset must hand the same slot back")
	assert.False(t, reused.selfdestructed)
	assert.False(t, reused.deleted)
	assert.False(t, reused.dirtyCode)
	assert.Zero(t, reused.data.Nonce)
	assert.Nil(t, reused.code.Bytes)
	assert.Nil(t, reused.dirtyStorage, "hand-out re-establishes the slot, dropping the map it held")
	assert.True(t, reused.arena, "hand-out re-tags the slot")
}

// TestStateObjectArenaFallsBackToHeap pins that the arena stops growing at its
// cap and signals the caller to allocate instead.
func TestStateObjectArenaFallsBackToHeap(t *testing.T) {
	var a stateObjectArena

	for i := range arenaMaxObjects {
		require.NotNil(t, a.alloc(), "slot %d must come from the arena", i)
	}
	require.Nil(t, a.alloc(), "arena must refuse to grow past its cap")

	a.reset()
	require.NotNil(t, a.alloc(), "reset must make the arena usable again")
}

// TestStateObjectArenaPointersSurviveGrowth pins that a pointer handed out
// before the arena grows still refers to the same object afterwards.
func TestStateObjectArenaPointersSurviveGrowth(t *testing.T) {
	var a stateObjectArena

	first := a.alloc()
	first.data.Nonce = 42

	for range arenaSlabSize * 3 {
		require.NotNil(t, a.alloc())
	}

	assert.EqualValues(t, 42, first.data.Nonce, "growth must not relocate live slots")
}

// TestStateObjectArenaReleaseKeepsSlotOutOfPool pins that an arena slot reaching
// release() is reset but not handed to the shared pool.
func TestStateObjectArenaReleaseKeepsSlotOutOfPool(t *testing.T) {
	var a stateObjectArena

	so := a.alloc()
	so.data.Nonce = 7
	so.release()

	assert.Zero(t, so.data.Nonce, "release must reset the slot")
	assert.True(t, so.arena)
	require.Same(t, so, &a.slabs[0][0], "the slot stays owned by the arena")
}

// BenchmarkNoMaterializeTx models a parallel-exec transaction: reset, then read
// the account fields of a set of distinct accounts.
func BenchmarkNoMaterializeTx(b *testing.B) {
	acc := accounts.NewAccount()
	acc.Nonce = 3
	acc.Balance.SetUint64(77)

	addrs := make([]accounts.Address, 64)
	for i := range addrs {
		addrs[i] = accounts.InternAddress([20]byte{0xAB, byte(i), byte(i >> 8)})
	}

	ibs, vm := newNoMaterializeIBS(&anyAccountReader{acc: &acc})
	defer ibs.Close()

	b.ReportAllocs()
	for b.Loop() {
		startNoMaterializeTx(ibs, vm, 0)
		for _, a := range addrs {
			_, _ = ibs.GetBalance(a)
			_, _ = ibs.GetNonce(a)
			_, _ = ibs.GetCodeHash(a)
		}
	}
}

// TestStateObjectArenaResetCoversVaryingHighWater pins that a slot is clean on
// hand-out even when transactions consume different numbers of slots: a slot is
// reset by every generation that handed it out.
func TestStateObjectArenaResetCoversVaryingHighWater(t *testing.T) {
	var a stateObjectArena

	dirty := func(n int) {
		for range n {
			so := a.alloc()
			require.NotNil(t, so)
			so.data.Nonce = 1
			so.selfdestructed = true
			so.setState(accounts.InternKey([32]byte{0x01}), *uint256.NewInt(1))
		}
		a.reset()
	}

	for _, n := range []int{200, 50, 200, 1, 130} {
		dirty(n)
	}

	for i := range 200 {
		so := a.alloc()
		require.NotNil(t, so)
		require.Zero(t, so.data.Nonce, "slot %d handed out dirty", i)
		require.False(t, so.selfdestructed, "slot %d handed out dirty", i)
		require.Empty(t, so.dirtyStorage, "slot %d handed out dirty", i)
	}
}
