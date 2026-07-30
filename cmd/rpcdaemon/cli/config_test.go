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

package cli

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
)

// TestIsWebsocket tests if an incoming websocket upgrade request is detected properly.
func TestIsWebsocket(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)

	require.False(t, isWebsocket(r))
	r.Header.Set("upgrade", "websocket")
	require.False(t, isWebsocket(r))
	r.Header.Set("connection", "upgrade")
	require.True(t, isWebsocket(r))
	r.Header.Set("connection", "upgrade,keep-alive")
	require.True(t, isWebsocket(r))
	r.Header.Set("connection", " UPGRADE,keep-alive")
	require.True(t, isWebsocket(r))
}

func TestParseSocketUrl(t *testing.T) {
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("unix:///some/file/path.sock")
		require.NoError(t, err)
		require.Equal(t, "/some/file/path.sock", socketUrl.Host+socketUrl.EscapedPath())
	})
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("tcp://localhost:1234")
		require.NoError(t, err)
		require.Equal(t, "localhost:1234", socketUrl.Host+socketUrl.EscapedPath())
	})
}

// TestRemoteRulesEngineFinalizeDelegates guards the BAL-regeneration path on a
// datadir-less rpcdaemon: block replay runs Initialize and Finalize on the
// remote engine wrapper, so Finalize must delegate rather than panic.
func TestRemoteRulesEngineFinalizeDelegates(t *testing.T) {
	e := &remoteRulesEngine{engine: merge.New(ethash.NewFaker())}
	header := &types.Header{Number: *uint256.NewInt(1)} // zero difficulty → PoS header
	require.NotPanics(t, func() {
		_, err := e.Finalize(&chain.Config{}, header, nil, nil, nil, nil, nil, nil, false, log.New())
		require.NoError(t, err)
	})
}
