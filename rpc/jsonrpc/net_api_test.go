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

package jsonrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type peersCountingBackend struct {
	rpchelper.ApiBackend
	peersCalls int
}

func (b *peersCountingBackend) Peers(context.Context) ([]*p2p.PeerInfo, error) {
	b.peersCalls++
	return nil, errors.New("sentry unavailable")
}

func TestNetListeningDoesNotQueryPeers(t *testing.T) {
	backend := &peersCountingBackend{}
	api := NewNetAPIImpl(backend)

	listening, err := api.Listening(context.Background())
	require.NoError(t, err)
	require.True(t, listening)
	require.Zero(t, backend.peersCalls)
}
