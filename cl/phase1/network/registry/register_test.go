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

package registry

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// PTC votes are due at one point in the slot, so a single peer can forward the
// whole committee's votes in one burst.
func TestPayloadAttestationRateLimiterAdmitsFullCommitteeBurstFromOnePeer(t *testing.T) {
	condition := withPayloadAttestationRateLimiter()
	pid := peer.ID("ptc-peer")
	for i := range int(clparams.MaxPtcSize) {
		require.True(t, condition(pid, nil, clparams.GloasVersion), "vote %d", i)
	}
}
