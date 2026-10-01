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
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/remotedb"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func remoteHistoryDB(t *testing.T, db kv.TemporalRoDB) (*remotedb.DB, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == remoteproto.KV_HistoryStartFrom_FullMethodName {
			calls.Add(1)
		}
		return handler(ctx, req)
	}))
	remoteproto.RegisterKVServer(server, remotedbserver.NewKvServer(t.Context(), db, nil, nil, log.New()))
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })
	t.Cleanup(server.Stop)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve remote history: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///history-floors", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	remote, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), log.New(), remoteproto.NewKVClient(conn)).Open()
	require.NoError(t, err)
	t.Cleanup(remote.Close)
	return remote, &calls
}

func TestRemoteHistoryFloorCacheSharesPinnedViews(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{mode: prune.Mode{
		Initialised: true, History: prunedHistoryDistance, Blocks: prune.KeepAllBlocksPruneMode,
	}})
	apis.eth._txNumReader = rawdbv3.TxNums
	apis.eth._historyPruneFloor.ttl = time.Hour
	db, calls := remoteHistoryDB(t, apis.eth.db)
	read := func() historyPruneFloors {
		tx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		floor, err := apis.eth.historyStartBlocks(t.Context(), tx, chainInfo.head)
		require.NoError(t, err)
		return floor
	}
	first := read()
	require.Positive(t, first.startTxNum)
	require.Equal(t, first, read())
	require.Equal(t, int64(3), calls.Load(), "transactions with the same pinned view share one floor read")
}

func TestRemoteHistoryFloorCacheSeparatesPinnedFiles(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{mode: prune.Mode{
		Initialised: true, History: prunedHistoryDistance, Blocks: prune.KeepAllBlocksPruneMode,
	}})
	apis.eth._txNumReader = rawdbv3.TxNums
	apis.eth._historyPruneFloor.ttl = time.Hour
	ctx := t.Context()
	db, calls := remoteHistoryDB(t, apis.eth.db)
	before, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer before.Rollback()
	old, err := apis.eth.historyStartBlocks(ctx, before, chainInfo.head)
	require.NoError(t, err)

	local, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer local.Rollback()
	end := local.Debug().TxNumsInFiles(kv.AccountsDomain)
	retired, err := local.Debug().Retire(ctx, kv.RetireCutoffs{Default: end - prunedHistoryStepSize})
	require.NoError(t, err)
	require.Positive(t, retired)
	after, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer after.Rollback()
	require.Equal(t, before.ViewID(), after.ViewID(), "file retirement does not commit MDBX")
	newFloor, err := apis.eth.historyStartBlocks(ctx, after, chainInfo.head)
	require.NoError(t, err)
	require.Greater(t, newFloor.startTxNum, old.startTxNum)
	got, err := apis.eth.historyStartBlocks(ctx, before, chainInfo.head)
	require.NoError(t, err)
	require.Equal(t, old, got, "the older transaction still pins its retained history")
	require.Equal(t, int64(6), calls.Load(), "each pinned view is loaded once")
}

func TestRemoteHistoryFloorFollowsRenewal(t *testing.T) {
	commitmentCfg := statecfg.Schema.CommitmentDomain
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema.CommitmentDomain = commitmentCfg })

	for _, tc := range []struct {
		name         string
		domain       kv.Domain
		duringLookup bool
	}{
		{"state/before_lookup", kv.AccountsDomain, false},
		{"state/during_lookup", kv.AccountsDomain, true},
		{"commitment/before_lookup", kv.CommitmentDomain, false},
		{"commitment/during_lookup", kv.CommitmentDomain, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apis, chainInfo := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{mode: prune.Mode{
				Initialised: true, History: prunedHistoryDistance, Blocks: prune.KeepAllBlocksPruneMode,
				CommitmentHistory: prunedHistoryDistance,
			}})
			apis.eth._txNumReader = rawdbv3.TxNums
			apis.eth._historyPruneFloor.ttl = time.Hour
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				db, _ := remoteHistoryDB(t, apis.eth.db)
				tx, err := db.BeginTemporalRo(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				readUncached := apis.eth.readHistoryStartBlocks
				if tc.domain == kv.CommitmentDomain {
					readUncached = apis.eth.readCommitmentHistoryStartBlocks
				}
				read := func() (historyPruneFloors, error) {
					if tc.domain == kv.CommitmentDomain {
						return readUncached(ctx, tx, chainInfo.head)
					}
					return apis.eth.historyStartBlocks(ctx, tx, chainInfo.head)
				}
				oldViewID := tx.ViewID()
				old, err := readUncached(ctx, tx, chainInfo.head)
				require.NoError(t, err)
				require.Positive(t, old.startTxNum, "a nonzero floor must reach the txNum-to-block lookup")
				if !tc.duringLookup {
					_, err = read()
					require.NoError(t, err)
				}

				local, err := apis.eth.db.BeginTemporalRo(ctx)
				require.NoError(t, err)
				defer local.Rollback()
				end := local.Debug().TxNumsInFiles(tc.domain)
				retired, err := local.Debug().Retire(ctx, kv.RetireCutoffs{Default: end - prunedHistoryStepSize})
				require.NoError(t, err)
				require.Positive(t, retired)
				require.NoError(t, apis.rwDB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.DatabaseInfo, []byte("history-cache-test"), []byte{1})
				}))
				time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
				if !tc.duringLookup {
					cursor, err := tx.Cursor(kv.MaxTxNum)
					require.NoError(t, err)
					defer cursor.Close()
				}

				got, err := read()
				require.NoError(t, err)
				require.NotEqual(t, oldViewID, tx.ViewID(), "renewal refreshes the MDBX snapshot ID too")
				want, err := readUncached(ctx, tx, chainInfo.head)
				require.NoError(t, err)
				require.Greater(t, want.startTxNum, old.startTxNum)
				require.Equal(t, want, got, "the floor must match the renewed view")
			})
		})
	}
}
