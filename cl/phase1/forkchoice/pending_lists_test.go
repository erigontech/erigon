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

package forkchoice

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"

	lru "github.com/hashicorp/golang-lru/v2"
)

type pendingListsForkGraph struct {
	fork_graph.ForkGraph
	headers map[common.Hash]*cltypes.BeaconBlockHeader
}

func (g pendingListsForkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	header, ok := g.headers[root]
	return header, ok
}

func newPendingListsTestStore(t testing.TB, headers map[common.Hash]*cltypes.BeaconBlockHeader) *ForkChoiceStore {
	t.Helper()
	pendingConsolidations, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingConsolidation]](queueCacheSize)
	require.NoError(t, err)
	pendingDeposits, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingDeposit]](queueCacheSize)
	require.NoError(t, err)
	partialWithdrawals, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingPartialWithdrawal]](queueCacheSize)
	require.NoError(t, err)
	return &ForkChoiceStore{
		forkGraph:             pendingListsForkGraph{headers: headers},
		beaconCfg:             &clparams.MainnetBeaconConfig,
		pendingConsolidations: pendingConsolidations,
		pendingDeposits:       pendingDeposits,
		partialWithdrawals:    partialWithdrawals,
	}
}

func TestAddPendingListsPreservesCachedRoot(t *testing.T) {
	blockRoot := common.Hash{0x01}
	parentRoot := common.Hash{0x02}
	branches := []struct {
		name       string
		withHeader bool
		withParent bool
	}{
		{name: "without header"},
		{name: "with header and parent missing", withHeader: true},
		{name: "with different parent", withHeader: true, withParent: true},
	}

	for _, branch := range branches {
		t.Run("deposits/"+branch.name, func(t *testing.T) {
			headers := map[common.Hash]*cltypes.BeaconBlockHeader{}
			store := newPendingListsTestStore(t, headers)
			if branch.withHeader {
				headers[blockRoot] = &cltypes.BeaconBlockHeader{ParentRoot: parentRoot}
			}
			if branch.withParent {
				store.pendingDeposits.Add(parentRoot, solid.NewPendingDepositList(store.beaconCfg))
			}
			deposit := &solid.PendingDeposit{Amount: 1, Slot: 2}
			source := solid.NewPendingDepositList(store.beaconCfg)
			source.Append(deposit)
			cached, err := source.HashSSZ()
			require.NoError(t, err)
			deposit.Amount++
			fresh := solid.NewPendingDepositList(store.beaconCfg)
			fresh.Append(deposit)
			freshRoot, err := fresh.HashSSZ()
			require.NoError(t, err)
			require.NotEqual(t, cached, freshRoot)

			require.NoError(t, store.addPendingDeposits(blockRoot, source))
			stored, ok := store.GetPendingDeposits(blockRoot)
			require.True(t, ok)
			storedRoot, err := stored.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, cached, storedRoot)
		})

		t.Run("consolidations/"+branch.name, func(t *testing.T) {
			headers := map[common.Hash]*cltypes.BeaconBlockHeader{}
			store := newPendingListsTestStore(t, headers)
			if branch.withHeader {
				headers[blockRoot] = &cltypes.BeaconBlockHeader{ParentRoot: parentRoot}
			}
			if branch.withParent {
				store.pendingConsolidations.Add(parentRoot, solid.NewPendingConsolidationList(store.beaconCfg))
			}
			consolidation := &solid.PendingConsolidation{SourceIndex: 1, TargetIndex: 2}
			source := solid.NewPendingConsolidationList(store.beaconCfg)
			source.Append(consolidation)
			cached, err := source.HashSSZ()
			require.NoError(t, err)
			consolidation.SourceIndex++
			fresh := solid.NewPendingConsolidationList(store.beaconCfg)
			fresh.Append(consolidation)
			freshRoot, err := fresh.HashSSZ()
			require.NoError(t, err)
			require.NotEqual(t, cached, freshRoot)

			require.NoError(t, store.addPendingConsolidations(blockRoot, source))
			stored, ok := store.GetPendingConsolidations(blockRoot)
			require.True(t, ok)
			storedRoot, err := stored.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, cached, storedRoot)
		})

		t.Run("partial withdrawals/"+branch.name, func(t *testing.T) {
			headers := map[common.Hash]*cltypes.BeaconBlockHeader{}
			store := newPendingListsTestStore(t, headers)
			if branch.withHeader {
				headers[blockRoot] = &cltypes.BeaconBlockHeader{ParentRoot: parentRoot}
			}
			if branch.withParent {
				store.partialWithdrawals.Add(parentRoot, solid.NewPendingWithdrawalList(store.beaconCfg))
			}
			withdrawal := &solid.PendingPartialWithdrawal{ValidatorIndex: 1, Amount: 2, WithdrawableEpoch: 3}
			source := solid.NewPendingWithdrawalList(store.beaconCfg)
			source.Append(withdrawal)
			cached, err := source.HashSSZ()
			require.NoError(t, err)
			withdrawal.Amount++
			fresh := solid.NewPendingWithdrawalList(store.beaconCfg)
			fresh.Append(withdrawal)
			freshRoot, err := fresh.HashSSZ()
			require.NoError(t, err)
			require.NotEqual(t, cached, freshRoot)

			require.NoError(t, store.addPendingPartialWithdrawals(blockRoot, source))
			stored, ok := store.GetPendingPartialWithdrawals(blockRoot)
			require.True(t, ok)
			storedRoot, err := stored.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, cached, storedRoot)
		})
	}
}

func TestAddPendingDepositsIsolatesListMutations(t *testing.T) {
	blockRoot := common.Hash{0x01}
	store := newPendingListsTestStore(t, nil)
	originals := []*solid.PendingDeposit{
		{Amount: 1, Slot: 1},
		{Amount: 2, Slot: 2},
		{Amount: 3, Slot: 3},
	}
	source := solid.NewPendingDepositList(store.beaconCfg)
	for _, deposit := range originals {
		source.Append(deposit)
	}
	rootAtCopy, err := source.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, store.addPendingDeposits(blockRoot, source))
	stored, ok := store.GetPendingDeposits(blockRoot)
	require.True(t, ok)
	storedRoot, err := stored.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, rootAtCopy, storedRoot)

	source.Append(&solid.PendingDeposit{Amount: 4, Slot: 4})
	source.Cut(1)
	source.Set(0, &solid.PendingDeposit{Amount: 5, Slot: 5})
	require.Equal(t, len(originals), stored.Len())
	for i, deposit := range originals {
		require.Same(t, deposit, stored.Get(i))
	}
	storedRoot, err = stored.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, rootAtCopy, storedRoot)

	stored.Append(&solid.PendingDeposit{Amount: 6, Slot: 6})
	require.Equal(t, len(originals), source.Len())
}
