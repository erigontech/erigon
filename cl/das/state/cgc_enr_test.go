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

package peerdasstate

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// fulu p2p-interface.md: cgc is a "Uint64 big endian integer with no leading zero
// bytes (0 is encoded as empty byte string)".
func TestEncodeCgcMatchesTheEnrSpec(t *testing.T) {
	for _, tc := range []struct {
		cgc  uint64
		want []byte
	}{
		{0, []byte{}},
		{4, []byte{0x04}},
		{8, []byte{0x08}},
		{255, []byte{0xff}},
		{256, []byte{0x01, 0x00}},
	} {
		got := EncodeCgc(tc.cgc)
		require.Equal(t, tc.want, got, "cgc %d", tc.cgc)
		if len(got) > 0 {
			require.NotZero(t, got[0], "cgc %d has a leading zero byte", tc.cgc)
		}
	}
}

// fulu/validator.md requires a node whose custody requirement falls to keep advertising
// the previous (highest) count.
func TestSetCustodyGroupCountKeepsTheHighestAdvertised(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := NewPeerDasState(&cfg, &clparams.NetworkConfig{})

	require.True(t, s.SetCustodyGroupCount(8, func() uint64 { return 0 }), "an increase must be advertised")
	require.False(t, s.SetCustodyGroupCount(4, func() uint64 { return 0 }), "a decrease must not be advertised")
	require.Equal(t, uint64(8), s.GetAdvertisedCgc(), "the highest count must survive a decrease")
}

func TestSetCustodyGroupCountNeverLowersTheCustodyRaiseSlot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := NewPeerDasState(&cfg, &clparams.NetworkConfig{})

	require.True(t, s.SetCustodyGroupCount(96, func() uint64 { return 2001 }))
	require.True(t, s.SetCustodyGroupCount(128, func() uint64 { return 1001 }))
	require.Equal(t, uint64(2001), s.GetCustodyRaiseSlot())
}

func TestSetCustodyRaiseSlotNeverLowersIt(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := NewPeerDasState(&cfg, &clparams.NetworkConfig{})

	s.SetCustodyRaiseSlot(2001)
	s.SetCustodyRaiseSlot(1001)
	require.Equal(t, uint64(2001), s.GetCustodyRaiseSlot())
}

func TestRaiseAdvertisedCgcDoesNotChangeRealCgc(t *testing.T) {
	beaconConfig := clparams.MainnetBeaconConfig
	networkConfig := clparams.NetworkConfig{CgcKey: "cgc"}
	s := NewPeerDasState(&beaconConfig, &networkConfig)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	localNode := enode.NewLocalNode(nodeDB, key)
	s.SetLocalNodeID(localNode)

	require.True(t, s.RaiseAdvertisedCgc(96))
	require.Equal(t, beaconConfig.CustodyRequirement, s.GetRealCgc())
	require.Equal(t, uint64(96), s.GetAdvertisedCgc())
	var enrCgc []byte
	require.NoError(t, localNode.Node().Load(enr.WithEntry(networkConfig.CgcKey, &enrCgc)))
	require.Equal(t, EncodeCgc(96), enrCgc)

	require.False(t, s.RaiseAdvertisedCgc(8))
	require.Equal(t, uint64(96), s.GetAdvertisedCgc())
	require.NoError(t, localNode.Node().Load(enr.WithEntry(networkConfig.CgcKey, &enrCgc)))
	require.Equal(t, EncodeCgc(96), enrCgc)
}

func TestRaiseAdvertisedCgcIsNotLostToAConcurrentCustodyColumnsRead(t *testing.T) {
	beaconConfig := clparams.MainnetBeaconConfig
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&beaconConfig)
	}
	networkConfig := clparams.NetworkConfig{CgcKey: "cgc"}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	nodeDB, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(func() { nodeDB.Close() })
	localNode := enode.NewLocalNode(nodeDB, key)

	for range 1000 {
		s := NewPeerDasState(&beaconConfig, &networkConfig)
		s.SetLocalNodeID(localNode)
		var ready, readers sync.WaitGroup
		ready.Add(4)
		for range 4 {
			readers.Go(func() {
				ready.Done()
				_, _ = s.GetMyCustodyColumns()
			})
		}
		ready.Wait()
		s.RaiseAdvertisedCgc(beaconConfig.NumberOfCustodyGroups)
		readers.Wait()

		columns, err := s.GetMyCustodyColumns()
		require.NoError(t, err)
		require.Len(t, columns, int(beaconConfig.NumberOfColumns))
	}
}
