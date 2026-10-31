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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestValSkip_SelfDestructPromotionBumpsStamp(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	vm.InitStamps(32)

	addr := accounts.InternAddress(common.HexToAddress("0x18b2b7673c6d661923e9460d592699617828b293"))
	slot := accounts.InternKey(common.HexToHash("0x08"))
	val := *uint256.NewInt(0x1111)
	const k, j, i = 1, 3, 5

	vm.WriteStorage(addr, slot, Version{TxIndex: k}, val, true)

	sd := newWriteSet(&VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Version: Version{TxIndex: j}, Path: SelfDestructPath},
		Val:         true,
	})
	vm.FlushVersionedWrites(sd, false)

	rs := ReadSet{}
	rs.SetStorage(addr, slot, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: k}},
		Val:        val,
	})
	check := func(rv, wv Version) VersionValidity {
		if rv != wv {
			return VersionInvalid
		}
		return VersionValid
	}

	stamp := vm.WriteSeq()
	require.Equal(t, VersionValid, vm.ValidateReadSet(i, rs, check, false, ""),
		"worker-side validation passes while the destruct cell is Estimate")
	require.True(t, vm.QuietBelow(i, stamp), "nothing changed since the stamp")

	vm.MarkWritesValidated(sd, nil)

	require.NotEqual(t, VersionValid, vm.ValidateReadSet(i, rs, check, false, ""),
		"after the destruct is Validated, the read of the destroyed contract's slot is stale")
	require.False(t, vm.QuietBelow(i, stamp),
		"promoting a lifecycle cell must invalidate stamps taken before it")
}

func newStampedMap(n int) *VersionMap {
	vm := NewVersionMap(nil)
	vm.InitStamps(n)
	return vm
}

func TestQuietBelow_InFlightWriteBelowBlocks(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	vm.mutBegin(3)
	require.False(t, vm.QuietBelow(5, stamp), "a write in flight below i must block")
	vm.mutEnd(3)
}

func TestQuietBelow_NewerCompletedWriteBelowBlocks(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	require.True(t, vm.QuietBelow(5, stamp))
	vm.mutBegin(3)
	vm.mutEnd(3)
	require.False(t, vm.QuietBelow(5, stamp), "a write completed after the stamp must block")
	require.True(t, vm.QuietBelow(5, vm.WriteSeq()), "a stamp taken after it must not")
}

func TestQuietBelow_WriteAtOrAboveDoesNotBlock(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	for _, j := range []int{5, 6, 20, 31} {
		vm.mutBegin(j)
		require.True(t, vm.QuietBelow(5, stamp), "in-flight write at j=%d >= i must not block", j)
		vm.mutEnd(j)
		require.True(t, vm.QuietBelow(5, stamp), "completed write at j=%d >= i must not block", j)
	}
}

func TestQuietBelow_AdjacentBoundary(t *testing.T) {
	t.Parallel()
	const i = 5
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	vm.mutBegin(i - 1)
	require.False(t, vm.QuietBelow(i, stamp), "in-flight write at j=i-1 must block")
	vm.mutEnd(i - 1)
	require.False(t, vm.QuietBelow(i, stamp), "completed write at j=i-1 must block")

	vm = newStampedMap(32)
	stamp = vm.WriteSeq()
	vm.mutBegin(i)
	vm.mutEnd(i)
	require.True(t, vm.QuietBelow(i, stamp), "write at j=i must not block")
}

func TestQuietBelow_SystemTxWriterBlocks(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	vm.mutBegin(-1)
	require.False(t, vm.QuietBelow(0, stamp), "in-flight system-tx write must block tx 0")
	require.False(t, vm.QuietBelow(7, stamp), "in-flight system-tx write must block tx 7")
	vm.mutEnd(-1)
	require.False(t, vm.QuietBelow(0, stamp), "completed system-tx write must block tx 0")
	require.True(t, vm.QuietBelow(0, vm.WriteSeq()))
}

func TestQuietBelow_ZeroStampOrNoStampsNeverQuiet(t *testing.T) {
	t.Parallel()
	require.False(t, NewVersionMap(nil).QuietBelow(5, 1), "uninitialised stamps")
	vm := newStampedMap(32)
	require.False(t, vm.QuietBelow(5, 0), "zero stamp means not taken")
}

func TestQuietBelow_FlushBumpsStampOfWriterSlot(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	stamp := vm.WriteSeq()
	flushLifecycle(vm, addr, 2, BalancePath)
	require.False(t, vm.QuietBelow(3, stamp), "flush by tx 2 blocks tx 3")
	require.True(t, vm.QuietBelow(2, stamp), "flush by tx 2 does not block tx 2")
}

func flushLifecycle(vm *VersionMap, addr accounts.Address, tx int, path AccountPath) *WriteSet {
	h := WriteHeader{Address: addr, Version: Version{TxIndex: tx}, Path: path}
	var ws *WriteSet
	switch path {
	case IncarnationPath:
		ws = newWriteSet(&VersionedWrite[uint64]{WriteHeader: h, Val: 1})
	case CreateContractPath, SelfDestructPath:
		ws = newWriteSet(&VersionedWrite[bool]{WriteHeader: h, Val: true})
	default:
		ws = newWriteSet(&VersionedWrite[uint256.Int]{WriteHeader: h, Val: *uint256.NewInt(1)})
	}
	vm.FlushVersionedWrites(ws, false)
	return ws
}

func TestValSkip_LifecyclePromotionBumpsStamp(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x02"))
	for name, path := range map[string]AccountPath{
		"incarnation":    IncarnationPath,
		"createContract": CreateContractPath,
		"selfDestruct":   SelfDestructPath,
	} {
		t.Run(name, func(t *testing.T) {
			vm := newStampedMap(32)
			ws := flushLifecycle(vm, addr, 3, path)
			stamp := vm.WriteSeq()
			require.True(t, vm.QuietBelow(5, stamp))
			vm.MarkWritesValidated(ws, nil)
			require.False(t, vm.QuietBelow(5, stamp), "promotion of a %s cell must bump", name)
		})
	}
}

func TestValSkip_CompleteOnLifecycleCellBumps(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x03"))
	ws := flushLifecycle(vm, addr, 3, SelfDestructPath)
	vm.MarkWritesValidated(ws, nil)
	stamp := vm.WriteSeq()
	require.True(t, vm.QuietBelow(5, stamp))
	vm.MarkWritesComplete(ws)
	require.False(t, vm.QuietBelow(5, stamp), "Validated->Done on a SelfDestruct cell must bump")
}

func TestValSkip_PromotionWithoutLifecycleCellDoesNotBump(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x04"))
	ws := flushLifecycle(vm, addr, 3, BalancePath)
	stamp := vm.WriteSeq()
	vm.MarkWritesValidated(ws, nil)
	require.True(t, vm.QuietBelow(5, stamp), "Estimate->Validated on a plain account must not bump")
	vm.MarkWritesComplete(ws)
	require.True(t, vm.QuietBelow(5, stamp), "Validated->Done on a plain account must not bump")
}

func TestQuietBelow_DeleteBumpsStamp(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x05"))
	ops := map[string]func(vm *VersionMap, j int){
		"Delete":    func(vm *VersionMap, j int) { vm.Delete(addr, BalancePath, accounts.NilKey, j, true) },
		"DeleteAll": func(vm *VersionMap, j int) { vm.DeleteAll(addr, j) },
	}
	for name, op := range ops {
		for _, tc := range []struct {
			j     int
			quiet bool
		}{{3, false}, {4, false}, {5, true}, {9, true}} {
			vm := newStampedMap(32)
			flushLifecycle(vm, addr, tc.j, BalancePath)
			stamp := vm.WriteSeq()
			op(vm, tc.j)
			require.Equal(t, tc.quiet, vm.QuietBelow(5, stamp), "%s by j=%d against i=5", name, tc.j)
		}
	}
}

func TestQuietBelow_WritePrimitivesBumpStamp(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x07"))
	stamp := vm.WriteSeq()
	vm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1), true)
	require.False(t, vm.QuietBelow(5, stamp), "a Write* after InitStamps must stamp its writer")
}

func TestQuietBelow_NoOpBALFlushDoesNotBump(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x08"))
	vm := NewVersionMap(nil)
	vm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1), true)
	vm.HasBAL = true
	vm.InitStamps(32)
	stamp := vm.WriteSeq()
	flushLifecycle(vm, addr, 3, BalancePath)
	require.True(t, vm.QuietBelow(5, stamp), "a flush that changes no cell must not stamp")
}

func TestQuietBelow_WriterOutsideLayoutPanics(t *testing.T) {
	t.Parallel()
	const last = 7
	vm := newStampedMap(last)
	require.Panics(t, func() { vm.mutBegin(last + 1) })
	require.Panics(t, func() { vm.mutBegin(-2) })
	vm.mutBegin(last)
	vm.mutEnd(last)
}

func TestQuietBelow_StampsSizedForLastIndex(t *testing.T) {
	t.Parallel()
	const last = 7
	vm := newStampedMap(last)
	stamp := vm.WriteSeq()
	require.True(t, vm.QuietBelow(-1, stamp), "reader -1 has no writer below it")
	require.True(t, vm.QuietBelow(last, stamp))
	vm.mutBegin(last - 1)
	vm.mutEnd(last - 1)
	require.False(t, vm.QuietBelow(last, stamp), "a write by L-1 must block reader L")
	require.True(t, vm.QuietBelow(-1, stamp), "reader -1 is never blocked")

	vm = newStampedMap(last)
	stamp = vm.WriteSeq()
	vm.mutBegin(last)
	vm.mutEnd(last)
	require.True(t, vm.QuietBelow(last, stamp), "a write by L does not block reader L")
}
