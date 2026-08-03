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
	ibs.SetTxContext(1, txIndex)
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

	for txIndex := range 200 {
		startNoMaterializeTx(ibs, vm, txIndex)
		balance, err := ibs.GetBalance(addr)
		require.NoError(t, err)
		require.EqualValues(t, 77, balance.Uint64())
		require.Empty(t, ibs.stateObjects, "parallel read must not cache a stateObject")
	}

	require.Len(t, ibs.arena.slabs, 1, "200 transactions must reuse one slab")
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
	assert.True(t, ibs.allocStateObject().arena, "parallel path must use the arena")
}

// TestNoMaterializeOverflowSkipsPool pins that once the arena is full the
// parallel path allocates rather than draining the shared pool, since objects it
// hands out are never released back.
func TestNoMaterializeOverflowSkipsPool(t *testing.T) {
	ibs, _ := newNoMaterializeIBS(&emptyReader{})
	defer ibs.Close()
	ibs.SetNoMaterialize(true)

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

	//goland:noinspection GoBoolExpressions
	if !race.Enabled {
		require.LessOrEqual(t, allocs, 5.0, "stateObject must not be heap-allocated per transaction")
	}
}

// TestStateObjectArenaRecyclesSlots pins that rewind hands the same backing
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

	a.rewind()

	reused := a.alloc()
	require.Same(t, first, reused, "rewind must hand the same slot back")
	assert.False(t, reused.selfdestructed)
	assert.False(t, reused.deleted)
	assert.False(t, reused.dirtyCode)
	assert.Zero(t, reused.data.Nonce)
	assert.Nil(t, reused.code.Bytes)
	assert.Empty(t, reused.dirtyStorage)
	assert.NotNil(t, reused.dirtyStorage, "a map, once made, is kept and cleared rather than dropped")
	assert.True(t, reused.arena, "slot identity survives reset")
}

// TestStateObjectArenaFallsBackToHeap pins that the arena stops growing at its
// cap and signals the caller to allocate instead.
func TestStateObjectArenaFallsBackToHeap(t *testing.T) {
	var a stateObjectArena

	for i := range arenaMaxObjects {
		require.NotNil(t, a.alloc(), "slot %d must come from the arena", i)
	}
	require.Nil(t, a.alloc(), "arena must refuse to grow past its cap")

	a.rewind()
	require.NotNil(t, a.alloc(), "rewind must make the arena usable again")
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

// TestStateObjectArenaRewindCoversVaryingHighWater pins that a slot is clean on
// hand-out even when transactions consume different numbers of slots: a slot is
// reset by the rewind of every generation that handed it out.
func TestStateObjectArenaRewindCoversVaryingHighWater(t *testing.T) {
	var a stateObjectArena

	dirty := func(n int) {
		for range n {
			so := a.alloc()
			require.NotNil(t, so)
			so.data.Nonce = 1
			so.selfdestructed = true
			so.setState(accounts.InternKey([32]byte{0x01}), *uint256.NewInt(1))
		}
		a.rewind()
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
