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

package sentinelflags

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The upstream ethereum-package Caplin launcher starts the standalone sentinel with
// --sentinel.tcp.port=4001 --discovery.port=4001, so the default QUIC port must not
// collide with either of those, or with the standalone sentinel's own defaults.
func TestSentinelQUICPortDefaultAvoidsUpstreamLauncherConflict(t *testing.T) {
	require.Equal(t, uint(4002), SentinelQUICPort.Value)
	require.NotEqual(t, uint(SentinelDiscoveryPort.Value), SentinelQUICPort.Value)
	require.NotEqual(t, SentinelTcpPort.Value, SentinelQUICPort.Value)
}
