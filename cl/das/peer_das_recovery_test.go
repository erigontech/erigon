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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestPeerDasRecoveryReservationCoversQueuedAndRunningWork(t *testing.T) {
	root := common.Hash{1}
	peerDas := &peerdas{isRecovering: make(map[common.Hash]bool)}

	require.True(t, peerDas.reserveRecovery(root))
	require.False(t, peerDas.reserveRecovery(root))
	peerDas.releaseRecovery(root)
	require.True(t, peerDas.reserveRecovery(root))
}
