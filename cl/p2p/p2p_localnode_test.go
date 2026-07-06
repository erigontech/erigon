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

package p2p

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestNewUDPv5ListenerAdvertisesBoundUDPPort(t *testing.T) {
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	cfg := &P2PConfig{
		IpAddr:  "127.0.0.1",
		Port:    0,
		TCPPort: 4001,
		TmpDir:  t.TempDir(),
	}
	listener, err := NewUDPv5Listener(context.Background(), cfg, discover.Config{PrivateKey: privKey}, log.Root())
	require.NoError(t, err)
	defer listener.Close()

	var udp enr.UDP
	require.NoError(t, listener.LocalNode().Node().Load(&udp))
	require.NotZero(t, udp)
}

func TestNewLocalNodeOmitsZeroTCPPort(t *testing.T) {
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	localNode, err := newLocalNode(context.Background(), privKey, net.ParseIP("127.0.0.1"), nil, 9000, 0, t.TempDir(), log.Root())
	require.NoError(t, err)
	defer localNode.Database().Close()

	var tcp enr.TCP
	err = localNode.Node().Load(&tcp)
	require.True(t, enr.IsNotFound(err))
}
