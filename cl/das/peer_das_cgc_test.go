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

package das

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestPeerDasRestoresHighestCustodyGroupCount(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	first, _ := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 1000)
	first.UpdateValidatorsCustody(96)

	secondNode := newCgcTestLocalNode(t)
	second, secondState := newCgcTestPeerDas(t, db, secondNode, 1000)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	second.Start(ctx)

	require.Equal(t, uint64(96), secondState.GetAdvertisedCgc())
	var enrCgc []byte
	require.NoError(t, secondNode.Node().Load(enr.WithEntry(clparams.NetworkConfigs[chainspec.MainnetChainID].CgcKey, &enrCgc)))
	require.Equal(t, peerdasstate.EncodeCgc(96), enrCgc)
}

func TestPeerDasStartWithoutStoredCustodyGroupCountKeepsRequirement(t *testing.T) {
	peerDas, state := newCgcTestPeerDas(t, mdbxtest.NewTestDB(t, dbcfg.ChainDB), newCgcTestLocalNode(t), 1000)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	peerDas.Start(ctx)

	require.Equal(t, clparams.MainnetBeaconConfig.CustodyRequirement, state.GetAdvertisedCgc())
}

// fulu/validator.md: after a custody group count change, earliest_available_slot SHOULD
// reflect the slot of the change. Older slots lack the columns of the added groups.
func TestPeerDasCustodyRaiseMovesEarliestAvailableSlot(t *testing.T) {
	peerDas, state := newCgcTestPeerDas(t, mdbxtest.NewTestDB(t, dbcfg.ChainDB), newCgcTestLocalNode(t), 1000)
	state.SetEarliestAvailableSlot(100)

	peerDas.UpdateValidatorsCustody(clparams.MainnetBeaconConfig.SamplesPerSlot + 1)
	require.Equal(t, uint64(1001), state.GetEarliestAvailableSlot())

	require.NoError(t, peerDas.PruneBelow(200))
	require.Equal(t, uint64(1001), state.GetEarliestAvailableSlot(), "the prune floor must not lower it below the raise")
}

// A node already stores SAMPLES_PER_SLOT columns, so a raise up to that count adds none.
func TestPeerDasCustodyRaiseWithinSampledColumnsKeepsEarliestAvailableSlot(t *testing.T) {
	peerDas, state := newCgcTestPeerDas(t, mdbxtest.NewTestDB(t, dbcfg.ChainDB), newCgcTestLocalNode(t), 1000)
	state.SetEarliestAvailableSlot(100)

	peerDas.UpdateValidatorsCustody(clparams.MainnetBeaconConfig.SamplesPerSlot)
	require.Equal(t, uint64(100), state.GetEarliestAvailableSlot())
}

func TestPeerDasRestoresCustodyRaiseSlot(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	first, _ := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 1000)
	first.UpdateValidatorsCustody(96)

	second, secondState := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 5000)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	second.Start(ctx)
	require.NoError(t, second.PruneBelow(200))
	require.Equal(t, uint64(1001), secondState.GetEarliestAvailableSlot())
}

// A raise that adds no custody columns must not leave a raise slot for the restart to restore.
func TestPeerDasRestartAfterRaiseWithinSampledColumnsKeepsEarliestAvailableSlot(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	first, _ := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 1000)
	first.UpdateValidatorsCustody(clparams.MainnetBeaconConfig.SamplesPerSlot)

	second, secondState := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 5000)
	second.Start(t.Context())
	require.NoError(t, second.PruneBelow(200))
	require.Equal(t, uint64(200), secondState.GetEarliestAvailableSlot())
}

// A count stored by a version that did not store a raise slot is treated as a raise on restore.
func TestPeerDasStartRecordsRaiseSlotForCountStoredWithoutOne(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestCustodyGroupCount(tx, 96)
	}))

	first, firstState := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 5000)
	first.Start(t.Context())
	require.NoError(t, first.PruneBelow(200))
	require.Equal(t, uint64(5001), firstState.GetEarliestAvailableSlot())

	second, secondState := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 9000)
	second.Start(t.Context())
	require.NoError(t, second.PruneBelow(200))
	require.Equal(t, uint64(5001), secondState.GetEarliestAvailableSlot(), "the recorded raise slot must be persisted")
}

// Peers can read the earliest available slot while Start writes the recorded raise slot.
func TestPeerDasStartInstallsRecordedRaiseSlotBeforeWritingIt(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestCustodyGroupCount(tx, 96)
	}))

	probe := &updateProbeDB{RwDB: db}
	peerDas, state := newCgcTestPeerDas(t, probe, newCgcTestLocalNode(t), 5000)
	var earliestAtWrite uint64
	probe.beforeUpdate = func() { earliestAtWrite = state.GetEarliestAvailableSlot() }
	peerDas.Start(t.Context())
	require.Equal(t, uint64(5001), earliestAtWrite)
}

// A raiser that read the slot before installing the raise could stall across a slot boundary
// and record a slot whose block was already checked with the old custody columns.
func TestPeerDasReadsCustodyRaiseSlotAfterInstallingRaise(t *testing.T) {
	localNode := newCgcTestLocalNode(t)
	cgcKey := clparams.NetworkConfigs[chainspec.MainnetChainID].CgcKey
	var raiseInstalledAtRead bool
	peerDas, _ := newCgcTestPeerDasWithClock(t, mdbxtest.NewTestDB(t, dbcfg.ChainDB), localNode, func() uint64 {
		var enrCgc []byte
		raiseInstalledAtRead = localNode.Node().Load(enr.WithEntry(cgcKey, &enrCgc)) == nil && bytes.Equal(peerdasstate.EncodeCgc(96), enrCgc)
		return 1000
	})

	peerDas.UpdateValidatorsCustody(96)
	require.True(t, raiseInstalledAtRead, "the slot must be read after the raise is installed")
}

func TestPeerDasCustodyRaiseAfterClockStepsBackIsAboveForkChoiceSlot(t *testing.T) {
	clockSlot := uint64(1001)
	peerDas, state := newCgcTestPeerDasWithClock(t, mdbxtest.NewTestDB(t, dbcfg.ChainDB), newCgcTestLocalNode(t), func() uint64 { return clockSlot })
	peerDas.SetForkChoice(cgcTestForkChoice{slot: 1001})
	oldColumns, err := state.GetMyCustodyColumns()
	require.NoError(t, err)
	columnStorage := peerDas.(*peerdas).columnStorage.(*blob_storage_mock_services.MockDataColumnStorage)
	columnStorage.EXPECT().GetSavedColumnIndex(gomock.Any(), uint64(1001), gomock.Any()).Return(slices.Collect(maps.Keys(oldColumns)), nil).AnyTimes()
	available, err := peerDas.IsDataAvailable(1001, common.Hash{1})
	require.NoError(t, err)
	require.True(t, available)

	clockSlot = 1000
	peerDas.UpdateValidatorsCustody(clparams.MainnetBeaconConfig.SamplesPerSlot + 1)
	available, err = peerDas.IsDataAvailable(1001, common.Hash{1})
	require.NoError(t, err)
	require.False(t, available)
	require.Equal(t, uint64(1002), state.GetEarliestAvailableSlot(), "the block at 1001 lacks the added columns")
}

// At Start the fork choice is still at its anchor slot, behind the clock.
func TestPeerDasStartRecordsRaiseSlotFromClockAheadOfForkChoice(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteHighestCustodyGroupCount(tx, 96)
	}))

	peerDas, state := newCgcTestPeerDas(t, db, newCgcTestLocalNode(t), 5000)
	peerDas.SetForkChoice(cgcTestForkChoice{slot: 4000})
	peerDas.Start(t.Context())
	require.Equal(t, uint64(5001), state.GetEarliestAvailableSlot())
}

func newCgcTestPeerDas(t *testing.T, db kv.RwDB, localNode *enode.LocalNode, currentSlot uint64) (PeerDas, *peerdasstate.PeerDasState) {
	t.Helper()
	return newCgcTestPeerDasWithClock(t, db, localNode, func() uint64 { return currentSlot })
}

func newCgcTestPeerDasWithClock(t *testing.T, db kv.RwDB, localNode *enode.LocalNode, currentSlot func() uint64) (PeerDas, *peerdasstate.PeerDasState) {
	t.Helper()
	beaconConfig := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&beaconConfig)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	ctrl := gomock.NewController(t)
	gossipManager := gossipmock.NewMockGossip(ctrl)
	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).AnyTimes()
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().DoAndReturn(currentSlot).AnyTimes()
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().PruneBelow(gomock.Any()).Return(nil).AnyTimes()
	columnStorage.EXPECT().EarliestStoredSlot().Return(uint64(150), nil).AnyTimes()
	state := peerdasstate.NewPeerDasState(&beaconConfig, &networkConfig)
	state.SetLocalNodeID(localNode)
	return NewPeerDas(nil, &beaconConfig, &clparams.CaplinConfig{}, columnStorage, nil, nil, localNode.ID(), clock, state, gossipManager, nil, db), state
}

type cgcTestForkChoice struct{ slot uint64 }

func (cgcTestForkChoice) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) { return nil, false }

func (f cgcTestForkChoice) Slot() uint64 { return f.slot }

type updateProbeDB struct {
	kv.RwDB
	beforeUpdate func()
}

func (db *updateProbeDB) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	db.beforeUpdate()
	return db.RwDB.Update(ctx, f)
}

func newCgcTestLocalNode(t testing.TB) *enode.LocalNode {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	return enode.NewLocalNode(nodeDB, key)
}
