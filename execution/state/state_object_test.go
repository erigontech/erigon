// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A SetCode revert restores the code a prior tx published, with its hash, even when the account
// record holds an older hash or the code was cleared to empty.
func TestSetCodeRevertRestoresCodeHash(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))
	oldCode := accounts.NewCode([]byte{0x60, 0x01})
	for _, tc := range []struct {
		name string
		code accounts.Code
	}{
		{"published", accounts.NewCode([]byte{0x60, 0x02})},
		{"cleared", accounts.NewCode(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A materialized object needs the non-arena state; it still carries a version map.
			ibs := NewForSequentialCommitment(NewNoopReader())
			defer ibs.Close()
			ibs.VersionMap().WriteCode(addr, Version{TxIndex: 0}, tc.code, true)
			ibs.SetTxContext(1, 1)

			acc := accounts.NewAccount()
			acc.CodeHash = oldCode.Hash
			so := newObject(ibs, addr, &acc, &acc)
			ibs.setStateObject(addr, so)

			snapshot := ibs.PushSnapshot()
			_, err := so.SetCode(accounts.NewCode([]byte{0x60, 0x03}), false, tracing.CodeChangeUnspecified)
			require.NoError(t, err)
			ibs.RevertToSnapshot(snapshot, nil)

			code, err := so.CodeTyped()
			require.NoError(t, err)
			require.Equal(t, tc.code, code)
			require.Equal(t, tc.code.Hash, so.data.CodeHash)
			require.Equal(t, tc.code.Hash, so.original.CodeHash)
		})
	}
}

func BenchmarkCutOriginal(b *testing.B) {
	value := common.HexToHash("0x01")
	for b.Loop() {
		bytes.TrimLeft(value[:], "\x00")
	}
}

func BenchmarkCutsetterFn(b *testing.B) {
	value := common.HexToHash("0x01")
	cutSetFn := func(r rune) bool { return r == 0 }
	for b.Loop() {
		bytes.TrimLeftFunc(value[:], cutSetFn)
	}
}

func BenchmarkCutCustomTrim(b *testing.B) {
	value := common.HexToHash("0x01")
	for b.Loop() {
		common.TrimLeftZeroes(value[:])
	}
}

var stateObjectSink *stateObject

// Most state objects never write storage, so a new one allocates its storage
// maps only on the first write.
func TestNewStateObjectAllocatesOnlyItself(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		stateObjectSink = stateObjectPool.New().(*stateObject)
	})
	if allocs != 1 {
		t.Fatalf("a new state object made %v allocations, want 1", allocs)
	}
}

// Close hands back everything a call collected, origin cells included, so the
// next state takes them from the pools instead of the heap.
func TestCloseReleasesOriginCells(t *testing.T) {
	ibs := New(NewNoopReader())
	ibs.SetVersionMap(NewVersionMap(nil))
	ibs.recordStorageOrigin(accounts.InternAddress([20]byte{0xc0, 0xde}), accounts.InternKey([32]byte{1}), uint256.Int{})
	ibs.Close()
	if !ibs.versionedOrigins.IsEmpty() {
		t.Fatal("Close kept the origin cells")
	}
}
