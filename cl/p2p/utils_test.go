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
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
)

func TestConvertToSingleMultiAddrRejectsNodeWithoutTcpPort(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	noTcp := enode.NewV4(&key.PublicKey, net.ParseIP("192.0.2.2"), 0, 30301)

	_, err = ConvertToSingleMultiAddr(noTcp)
	require.Error(t, err)
}

func TestConvertToMultiAddrSkipsNodesWithoutTcpPort(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	withTcp := enode.NewV4(&key.PublicKey, net.ParseIP("192.0.2.1"), 30303, 30301)
	noTcp := enode.NewV4(&key.PublicKey, net.ParseIP("192.0.2.2"), 0, 30301)

	multiAddrs := ConvertToMultiAddr([]*enode.Node{withTcp, noTcp})

	require.Len(t, multiAddrs, 1)
	require.Contains(t, multiAddrs[0].String(), "/tcp/30303")
}
