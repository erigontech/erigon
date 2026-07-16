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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
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

func TestZeroBudgetRemoteCachePinsCommittedState(t *testing.T) {
	cfg := kvcache.DefaultCoherentConfig
	cfg.CacheSize = 0
	cfg.CodeCacheSize = 0
	cache := newRemoteStateCache(cfg)

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	addr := common.Address{1}
	committedAccount := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	announcedAccount := committedAccount
	announcedAccount.Nonce = 2
	committedData := accounts.SerialiseV3(&committedAccount)
	announcedData := accounts.SerialiseV3(&announcedAccount)

	require.NoError(t, db.UpdateTemporal(t.Context(), func(tx kv.TemporalRwTx) error {
		domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
		if err != nil {
			return err
		}
		defer domains.Close()
		if err := domains.DomainPut(kv.AccountsDomain, tx, addr[:], committedData, 0, nil); err != nil {
			return err
		}
		return domains.Flush(t.Context(), tx)
	}))

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	stateVersion, err := tx.ReadSequence(string(kv.PlainStateVersion))
	require.NoError(t, err)

	cache.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: stateVersion + 1,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    announcedData,
			}},
		}},
	})
	require.Zero(t, cache.Len())

	view, err := cache.View(t.Context(), tx)
	require.NoError(t, err)
	data, err := view.Get(addr[:])
	require.NoError(t, err)
	require.Equal(t, committedData, data)
}
