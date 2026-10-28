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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
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

func newCgcTestPeerDas(t *testing.T, db kv.RwDB, localNode *enode.LocalNode, currentSlot uint64) (PeerDas, *peerdasstate.PeerDasState) {
	t.Helper()
	beaconConfig := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&beaconConfig)
	networkConfig := clparams.NetworkConfigs[chainspec.MainnetChainID]
	ctrl := gomock.NewController(t)
	gossipManager := gossipmock.NewMockGossip(ctrl)
	gossipManager.EXPECT().SubscribeWithExpiry(gomock.Any(), gomock.Any()).AnyTimes()
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().PruneBelow(gomock.Any()).Return(nil).AnyTimes()
	columnStorage.EXPECT().EarliestStoredSlot().Return(uint64(150), nil).AnyTimes()
	state := peerdasstate.NewPeerDasState(&beaconConfig, &networkConfig)
	state.SetLocalNodeID(localNode)
	return NewPeerDas(nil, &beaconConfig, &clparams.CaplinConfig{}, columnStorage, nil, nil, localNode.ID(), clock, state, gossipManager, nil, db), state
}

func newCgcTestLocalNode(t *testing.T) *enode.LocalNode {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	return enode.NewLocalNode(nodeDB, key)
}
