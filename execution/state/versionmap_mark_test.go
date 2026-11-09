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
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func balanceWrite(addr accounts.Address, v Version, val uint64) *WriteSet {
	return newWriteSet(&VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Key: accounts.NilKey, Version: v},
		Val:         *uint256.NewInt(val),
	})
}

func balanceFlag(t *testing.T, vm *VersionMap, addr accounts.Address, txIdx int) statusFlag {
	t.Helper()
	c, ok := vm.load(addr).Balance.Get(txIdx)
	require.True(t, ok)
	return c.loadFlag()
}

func TestMarkWritesValidatedDoesNotWaitForTheEntryLock(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress(common.HexToAddress("0x10"))
	ws := flushPath(vm, addr, 3, BalancePath)
	e := vm.load(addr)
	e.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		vm.MarkWritesValidated(ws, nil)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		e.mu.Unlock()
		<-done
		t.Fatal("the mark waited for the entry lock")
	}
	e.mu.Unlock()
	require.Equal(t, FlagValidated, balanceFlag(t, vm, addr, 3))
}

func TestMarkWritesValidatedLocksADestructedAccount(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress(common.HexToAddress("0x11"))
	flushPath(vm, addr, 1, SelfDestructPath)
	ws := flushPath(vm, addr, 3, BalancePath)
	e := vm.load(addr)
	e.mu.RLock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		vm.MarkWritesValidated(ws, nil)
	}()
	select {
	case <-done:
		e.mu.RUnlock()
		t.Fatal("a destructed account was marked while a reader held its lock")
	case <-time.After(50 * time.Millisecond):
	}
	e.mu.RUnlock()
	<-done
	require.Equal(t, FlagValidated, balanceFlag(t, vm, addr, 3))
}

func TestMarkWritesValidatedPanicsOnANewerIncarnation(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress(common.HexToAddress("0x12"))
	stale := balanceWrite(addr, Version{TxIndex: 3}, 1)
	vm.FlushVersionedWrites(stale, false)
	vm.FlushVersionedWrites(balanceWrite(addr, Version{TxIndex: 3, Incarnation: 1}, 2), false)
	require.Panics(t, func() { vm.MarkWritesValidated(stale, nil) })
	require.Equal(t, FlagEstimate, balanceFlag(t, vm, addr, 3))
}

func TestMarkWritesMarksAWriteSetMergedBeforeItsFlush(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mark func(*VersionMap, *WriteSet)
		want statusFlag
	}{
		"validated": {func(vm *VersionMap, ws *WriteSet) { vm.MarkWritesValidated(ws, nil) }, FlagValidated},
		"complete":  {func(vm *VersionMap, ws *WriteSet) { vm.MarkWritesComplete(ws) }, FlagDone},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			vm := NewVersionMap(nil)
			a := accounts.InternAddress(common.HexToAddress("0x13"))
			b := accounts.InternAddress(common.HexToAddress("0x14"))
			first, second := balanceWrite(a, Version{TxIndex: 3}, 1), balanceWrite(b, Version{TxIndex: 3}, 2)
			merged := first.Merge(second)
			vm.FlushVersionedWrites(first, false)
			vm.FlushVersionedWrites(second, false)
			tc.mark(vm, merged)
			require.Equal(t, tc.want, balanceFlag(t, vm, a, 3))
			require.Equal(t, tc.want, balanceFlag(t, vm, b, 3))
		})
	}
}
