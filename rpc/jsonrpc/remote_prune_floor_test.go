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

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/remotedb"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/state"
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

type expiringHistoryCursorTx struct {
	kv.TemporalTx
	expire  func()
	onClose bool
}

func (tx *expiringHistoryCursorTx) Cursor(bucket string) (kv.Cursor, error) {
	if bucket != kv.MaxTxNum || tx.expire == nil {
		return tx.TemporalTx.Cursor(bucket)
	}
	expire := tx.expire
	tx.expire = nil
	if !tx.onClose {
		expire()
		return tx.TemporalTx.Cursor(bucket)
	}
	cursor, err := tx.TemporalTx.Cursor(bucket) //nolint:gocritic // Ownership passes to the caller.
	if err != nil {
		return nil, err
	}
	return &expiringHistoryCursor{Cursor: cursor, expire: expire}, nil
}

type expiringHistoryCursor struct {
	kv.Cursor
	expire func()
}

func (c *expiringHistoryCursor) Close() {
	if c.expire != nil {
		c.expire()
		c.expire = nil
	}
	c.Cursor.Close()
}

func TestIndexedHistoryGateFollowsRemoteRenewal(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"open", "close"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
			apis.eth._txNumReader = rawdbv3.TxNums
			apis.eth._historyPruneFloor.ttl = time.Hour
			const block, index = uint64(8), uint64(1)
			local, err := apis.eth.db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer local.Rollback()
			minTxNum, err := apis.eth._txNumReader.Min(t.Context(), local, block)
			require.NoError(t, err)
			start := minTxNum + index + 1
			require.NoError(t, apis.rwDB.Update(t.Context(), func(tx kv.RwTx) error {
				return writeHistoryStart(tx, start)
			}))

			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				db, calls := remoteHistoryDB(t, apis.eth.db)
				tx, err := db.BeginTemporalRo(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				old, err := apis.eth.historyStartBlocks(ctx, tx, chainInfo.head)
				require.NoError(t, err)
				require.Equal(t, start, old.startTxNum)
				require.Greater(t, old.replay, block, "the indexed check must use its exact fallback")
				require.NoError(t, apis.eth.checkPruneTransactionHistoryAtIndex(ctx, tx, block, index))
				require.EqualValues(t, 3, calls.Load(), "the gate must reuse the cached floor")
				oldViewID := tx.ViewID()
				require.NoError(t, apis.rwDB.Update(ctx, func(tx kv.RwTx) error {
					return writeHistoryStart(tx, start+1)
				}))

				// The head reads must stay on the old view. Expire it only at the
				// indexed cursor, after reading the cached history floor.
				view := &expiringHistoryCursorTx{TemporalTx: tx, onClose: phase == "close", expire: func() {
					time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
				}}
				err = apis.eth.checkPruneTransactionHistoryAtIndex(ctx, view, block, index)
				require.Nil(t, view.expire, "the indexed fallback must open a cursor")
				require.NotEqual(t, oldViewID, tx.ViewID(), "the remote transaction must renew during the indexed check")
				require.ErrorIs(t, err, state.ErrPruned, "the renewed view no longer retains the requested pre-state")
				require.EqualValues(t, 6, calls.Load(), "renewal must load the floor once for the new view")
			})
		})
	}
}

func TestReceiptHistoryGateFollowsRemoteRenewal(t *testing.T) {
	commitmentCfg := statecfg.Schema.CommitmentDomain
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema.CommitmentDomain = commitmentCfg })

	for _, phase := range []string{"open", "close"} {
		t.Run(phase, func(t *testing.T) {
			apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
				mode: prune.ArchiveMode, chainConfig: byzantiumChainConfig(pruneGatingChainLen + 1),
			})
			apis.eth._txNumReader = rawdbv3.TxNums
			apis.eth._historyPruneFloor.ttl = time.Hour
			const block = uint64(8)
			local, err := apis.eth.db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer local.Rollback()
			_, err = apis.eth.chainConfig(t.Context(), local)
			require.NoError(t, err)
			start, err := apis.eth._txNumReader.Min(t.Context(), local, block)
			require.NoError(t, err)
			require.NoError(t, apis.rwDB.Update(t.Context(), func(tx kv.RwTx) error {
				if err := writeHistoryStart(tx, start); err != nil {
					return err
				}
				if err := tx.ClearTable(kv.TblCommitmentHistoryKeys); err != nil {
					return err
				}
				if err := tx.Put(kv.TblCommitmentHistoryKeys, hexutil.EncodeTs(start), []byte{1}); err != nil {
					return err
				}
				return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
			}))

			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				db, _ := remoteHistoryDB(t, apis.eth.db)
				tx, err := db.BeginTemporalRo(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				old, err := apis.eth.historyStartBlocks(ctx, tx, chainInfo.head)
				require.NoError(t, err)
				require.Equal(t, block, old.wholeBlock)
				require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, block))
				oldViewID := tx.ViewID()
				require.NoError(t, apis.rwDB.Update(ctx, func(tx kv.RwTx) error {
					return writeHistoryStart(tx, start+1)
				}))

				// The state floor is cached; only the commitment lookup opens a
				// cursor. Renewal there must invalidate both parts of the receipt floor.
				view := &expiringHistoryCursorTx{TemporalTx: tx, onClose: phase == "close", expire: func() {
					time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
				}}
				err = apis.eth.checkReceiptsAvailable(ctx, view, block)
				require.Nil(t, view.expire)
				require.NotEqual(t, oldViewID, tx.ViewID())
				require.ErrorIs(t, err, state.ErrPruned, "the renewed view no longer retains the block's initial system transaction")
			})
		})
	}
}
