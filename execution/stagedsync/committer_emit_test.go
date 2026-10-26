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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestEmitTouched_ProducesExpectedUpdates pins the commitment fold the calculator runs
// per block: accumulateTouched + emitTouched read each touched account/slot straight from
// the version map and emit the leaf update, with no EIP knowledge. A present account emits
// its full {balance, nonce, codeHash}; one the write side resolved absent (self-destruct or
// EIP-161 empty) emits a leaf+subtree delete; a slot emits its value or a delete when zeroed.
func TestEmitTouched_ProducesExpectedUpdates(t *testing.T) {
	t.Parallel()

	const txIdx = 3
	const T = txIdx + 1 // baseTxNum 0 ⇒ GetAccount floor includes the write at txIdx
	ver := state.Version{TxIndex: txIdx}
	a := func(b byte) accounts.Address { return accounts.InternAddress([20]byte{b}) }
	k := func(b byte) accounts.StorageKey { return accounts.InternKey([32]byte{b}) }
	codeHash := accounts.NewCode([]byte{0x60, 0x00}).Hash
	acctKey := func(addr accounts.Address) string { v := addr.Value(); return string(v[:]) }
	storKey := func(addr accounts.Address, slot accounts.StorageKey) string {
		av, sv := addr.Value(), slot.Value()
		return string(av[:]) + string(sv[:])
	}
	full := func(bal uint64, nonce uint64, ch accounts.CodeHash) commitment.Update {
		return commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Balance: *uint256.NewInt(bal), Nonce: nonce, CodeHash: ch.Value()}
	}
	del := commitment.Update{Flags: commitment.DeleteUpdate, CodeHash: empty.CodeHash, DeleteStorageSubtree: true}
	storVal := func(v uint64) commitment.Update {
		b := uint256.NewInt(v).Bytes()
		u := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(b))}
		copy(u.Storage[:], b)
		return u
	}
	storDel := commitment.Update{Flags: commitment.DeleteUpdate}

	scenarios := []struct {
		name   string
		build  func() *state.WriteSet
		expect func() map[string]commitment.Update
	}{
		{"live account full update", func() *state.WriteSet {
			return newWS().bal(a(1), ver, *uint256.NewInt(100)).nonce(a(1), ver, 7).codeHash(a(1), ver, codeHash).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(1)): full(100, 7, codeHash)}
		}},
		{"storage plus account", func() *state.WriteSet {
			return newWS().bal(a(3), ver, *uint256.NewInt(5)).nonce(a(3), ver, 1).
				stor(a(3), k(1), ver, *uint256.NewInt(1)).stor(a(3), k(2), ver, *uint256.NewInt(2)).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(3)): full(5, 1, accounts.EmptyCodeHash), storKey(a(3), k(1)): storVal(1), storKey(a(3), k(2)): storVal(2)}
		}},
		{"storage cleared to zero", func() *state.WriteSet {
			return newWS().bal(a(9), ver, *uint256.NewInt(5)).nonce(a(9), ver, 1).stor(a(9), k(3), ver, uint256.Int{}).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(9)): full(5, 1, accounts.EmptyCodeHash), storKey(a(9), k(3)): storDel}
		}},
		{"self-destruct deletes leaf", func() *state.WriteSet {
			return newWS().selfDestruct(a(4), ver, true).inc(a(4), ver, 0).build()
		}, func() map[string]commitment.Update { return map[string]commitment.Update{acctKey(a(4)): del} }},
		{"EIP-161 empty removal via self-destruct encoding", func() *state.WriteSet {
			return newWS().selfDestruct(a(5), ver, true).build()
		}, func() map[string]commitment.Update { return map[string]commitment.Update{acctKey(a(5)): del} }},
		{"created contract", func() *state.WriteSet {
			return newWS().createContract(a(7), ver, true).bal(a(7), ver, *uint256.NewInt(9)).nonce(a(7), ver, 1).codeHash(a(7), ver, codeHash).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(7)): full(9, 1, codeHash)}
		}},
		{"zero-balance live touch (nonce keeps it alive)", func() *state.WriteSet {
			return newWS().bal(a(8), ver, uint256.Int{}).nonce(a(8), ver, 1).build()
		}, func() map[string]commitment.Update {
			return map[string]commitment.Update{acctKey(a(8)): full(0, 1, accounts.EmptyCodeHash)}
		}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			ws := sc.build()
			vm := state.NewVersionMap(nil)
			vm.SetBaseTxNum(0)
			vm.FlushVersionedWrites(ws, true, "")
			vm.RecordWriteKeysAtTxIndex(txIdx, false, ws)

			got := emitTouchedUpdates(t, vm, T, nil, vm.WritesAtTxIndex(txIdx, false))
			require.Equal(t, sc.expect(), got)
		})
	}
}
