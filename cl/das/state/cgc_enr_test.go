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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
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
// the previous (highest) count. No test can reach SetCustodyGroupCount through peerdas -
// the only production caller is on_block.go and every test route is a gomock stub - so
// the ratchet is pinned here directly.
func TestSetCustodyGroupCountKeepsTheHighestAdvertised(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := NewPeerDasState(&cfg, &clparams.NetworkConfig{})

	require.True(t, s.SetCustodyGroupCount(8), "an increase must be advertised")
	require.False(t, s.SetCustodyGroupCount(4), "a decrease must not be advertised")
	require.Equal(t, uint64(8), s.GetAdvertisedCgc(), "the highest count must survive a decrease")
}
