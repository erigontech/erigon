// Copyright 2024 The Erigon Authors
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

package sync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/p2p"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestBlockEventsSpamGuard(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	sg := newBlockEventsSpamGuard(logger)
	peerId1 := p2p.PeerIdFromUint64(1)
	peerId2 := p2p.PeerIdFromUint64(2)
	require.False(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId2, common.HexToHash("0x0"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x1"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x0"), 2))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x2"), 2))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x3"), 3))
}
