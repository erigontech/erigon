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
	"bytes"
	"context"
	"errors"
	"math/big"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestReadWithStableTxViewFollowsHistoryFileRenewal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		firstErr error
	}{
		{"result", nil},
		{"pruning_error", state.ErrPruned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apis, _ := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{mode: prune.Mode{
				Initialised: true, History: prunedHistoryDistance, Blocks: prune.KeepAllBlocksPruneMode,
			}})
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				db, _ := remoteHistoryDB(t, apis.eth.db)
				tx, err := db.BeginTemporalRo(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				viewID := tx.ViewID()
				generation := tx.Debug().(interface{ HistoryFilesGeneration() uint64 }).HistoryFilesGeneration()
				oldStart, err := state.StateHistoryStartTxNum(tx)
				require.NoError(t, err)

				calls := 0
				got, err := readWithStableTxView(ctx, tx, func() (uint64, error) {
					calls++
					start, err := state.StateHistoryStartTxNum(tx)
					if err != nil || calls > 1 {
						return start, err
					}
					cursor, err := tx.Cursor(kv.MaxTxNum)
					require.NoError(t, err)
					defer cursor.Close()
					local, err := apis.eth.db.BeginTemporalRo(ctx)
					require.NoError(t, err)
					defer local.Rollback()
					end := local.Debug().TxNumsInFiles(kv.AccountsDomain)
					retired, err := local.Debug().Retire(ctx, kv.RetireCutoffs{Default: end - prunedHistoryStepSize})
					require.NoError(t, err)
					require.Positive(t, retired)
					// Expire after opening the cursor so its deferred Close renews the view.
					time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
					return start, tc.firstErr
				})
				require.Equal(t, viewID, tx.ViewID(), "file retirement must not commit MDBX")
				require.Greater(t, tx.Debug().(interface{ HistoryFilesGeneration() uint64 }).HistoryFilesGeneration(), generation)
				require.Equal(t, 2, calls, "discard both results and errors from the old history-file view")
				require.NoError(t, err)
				want, err := state.StateHistoryStartTxNum(tx)
				require.NoError(t, err)
				require.Greater(t, want, oldStart)
				require.Equal(t, want, got)
			})
		})
	}
}

func TestReadWithStableTxViewRetryLimit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		renewals int
		wantErr  bool
	}{
		{"stable_on_last_attempt", 2, false},
		{"keeps_renewing", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &stableViewTestTx{}
			calls := 0
			result, err := readWithStableTxView(t.Context(), tx, func() (int, error) {
				calls++
				if calls <= tc.renewals {
					tx.viewID++
				}
				return 42, nil
			})
			require.Equal(t, 3, calls)
			if tc.wantErr {
				require.ErrorContains(t, err, "database view changed")
				require.Zero(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, 42, result)
			}
		})
	}
}

func TestReadWithStableTxViewCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancelAfter int
	}{
		{"before_read", 0},
		{"during_retry", 1},
		{"on_last_attempt", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelAfter == 0 {
				cancel()
			}
			tx := &stableViewTestTx{}
			calls := 0
			result, err := readWithStableTxView(ctx, tx, func() (int, error) {
				calls++
				tx.viewID++
				if calls == tc.cancelAfter {
					cancel()
				}
				return 42, nil
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, result)
			require.Equal(t, tc.cancelAfter, calls)
		})
	}
}

type stableViewTestTx struct {
	kv.TemporalTx
	viewID uint64
}

func (tx *stableViewTestTx) ViewID() uint64 { return tx.viewID }

func (tx *stableViewTestTx) Debug() kv.TemporalDebugTx { return nil }

// The returned function transfers rollback ownership to the RPC endpoint.
// Remote rollback releases a limiter permit on every call, so it must run only once.
func beginRemoteTxForRPC(t *testing.T, ctx context.Context, db kv.TemporalRoDB) (kv.TemporalTx, func()) {
	t.Helper()
	tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic // Test cleanup owns tx until handoff.
	require.NoError(t, err)
	handedOff := false
	t.Cleanup(func() {
		if !handedOff {
			tx.Rollback()
		}
	})
	return tx, func() { handedOff = true }
}

func TestIndexedStateReadUsesPinnedCanonicalBlock(t *testing.T) {
	for _, endpoint := range []string{"account", "storage", "traceCall", "traceCallMany"} {
		for _, requested := range []string{"original", "replacement", "missing_canonical_mapping"} {
			t.Run(endpoint+"/"+requested, func(t *testing.T) {
				m, blocks, _ := rpcdaemontest.CreateTestExecModule(t)
				block := blocks.Blocks[0]
				ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
				reader := freezeblocks.NewRemoteBlockReader(remoteproto.NewETHBACKENDClient(conn))
				db, _ := remoteHistoryDB(t, m.DB)
				api := NewPrivateDebugAPI(newBaseApiForTest(m), db, nil, &rpccfg.DebugApiConfig{})
				api._pruneMode.Store(&prune.ArchiveMode)
				api._blockReader = reader
				api._txNumReader = reader.TxnumReader()
				read := func(hash common.Hash) (any, error) {
					switch endpoint {
					case "account":
						return api.AccountAt(ctx, hash, 0, m.Address)
					case "storage":
						return api.StorageRangeAt(ctx, hash, 0, m.Address, nil, 10)
					}
					block := rpc.BlockNumberOrHashWithHash(hash, true)
					args := ethapi.CallArgs{From: &m.Address, To: &common.Address{1}, Gas: new(hexutil.Uint64(21000))}
					config := &tracersConfig.TraceConfig{Tracer: new("callTracer"), TxIndex: new(hexutil.Uint(0))}
					return streamedResult(func(stream *jsonstream.Stream) error {
						if endpoint == "traceCall" {
							return api.TraceCall(ctx, args, &block, config, stream)
						}
						return api.TraceCallMany(ctx, []Bundle{{Transactions: []ethapi.CallArgs{args}}}, StateContext{BlockNumber: block, TransactionIndex: new(0)}, config, stream)
					})
				}
				want, err := read(block.Hash())
				require.NoError(t, err)

				if requested == "missing_canonical_mapping" {
					require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
						return tx.Delete(kv.HeaderCanonical, hexutil.EncodeTs(block.NumberU64()))
					}))
				}
				tx, handOff := beginRemoteTxForRPC(t, ctx, db)
				viewID := tx.ViewID()
				replacement := block.Header()
				replacement.Extra = []byte("replacement")
				require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
					if err := rawdb.WriteHeader(tx, replacement); err != nil {
						return err
					}
					return rawdb.WriteCanonicalHash(tx, replacement.Hash(), block.NumberU64())
				}))
				pinnedHash, err := rawdb.ReadCanonicalHash(tx, block.NumberU64())
				require.NoError(t, err)
				if requested == "missing_canonical_mapping" {
					require.Zero(t, pinnedHash)
				} else {
					require.Equal(t, block.Hash(), pinnedHash)
				}
				liveHash, ok, err := reader.CanonicalHash(ctx, tx, block.NumberU64())
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, replacement.Hash(), liveHash)

				api.db = historyViewDB{TemporalRoDB: db, tx: tx}
				hash := block.Hash()
				if requested != "original" {
					hash = replacement.Hash()
				}
				handOff()
				got, err := read(hash)
				require.Equal(t, viewID, tx.ViewID(), "the KV transaction did not renew")
				if requested != "original" {
					require.ErrorContains(t, err, "canonical", "the replacement block is not canonical in the state view")
				} else {
					require.NoError(t, err)
					require.Equal(t, want, got)
				}
			})
		}
	}
}

func TestIndexedTraceBlockHashUsesPinnedView(t *testing.T) {
	for _, endpoint := range []string{"traceCall", "traceCallMany"} {
		t.Run(endpoint, func(t *testing.T) {
			m, chain, _ := rpcdaemontest.CreateTestExecModule(t)
			ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
			reader := freezeblocks.NewRemoteBlockReader(remoteproto.NewETHBACKENDClient(conn))
			db, _ := remoteHistoryDB(t, m.DB)
			tx, handOff := beginRemoteTxForRPC(t, ctx, db)
			viewID := tx.ViewID()
			target := chain.Blocks[len(chain.Blocks)-1]
			parent := chain.Blocks[len(chain.Blocks)-2]
			replacementParent := parent.Header()
			replacementParent.Extra = []byte("replacement")
			replacementTarget := target.Header()
			replacementTarget.ParentHash = replacementParent.Hash()
			require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
				require.NoError(t, rawdb.WriteHeader(tx, replacementParent))
				require.NoError(t, rawdb.WriteBody(tx, replacementParent.Hash(), parent.NumberU64(), parent.Body()))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, replacementParent.Hash(), parent.NumberU64()))
				require.NoError(t, rawdb.WriteHeader(tx, replacementTarget))
				require.NoError(t, rawdb.WriteBody(tx, replacementTarget.Hash(), target.NumberU64(), target.Body()))
				return rawdb.WriteCanonicalHash(tx, replacementTarget.Hash(), target.NumberU64())
			}))
			pinnedHash, err := rawdb.ReadCanonicalHash(tx, parent.NumberU64())
			require.NoError(t, err)
			require.Equal(t, parent.Hash(), pinnedHash)
			liveHash, found, err := reader.CanonicalHash(ctx, tx, parent.NumberU64())
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, replacementParent.Hash(), liveHash)
			require.NotEqual(t, pinnedHash, liveHash)

			api := NewPrivateDebugAPI(newBaseApiForTest(m), historyViewDB{TemporalRoDB: db, tx: tx}, nil, &rpccfg.DebugApiConfig{})
			api._pruneMode.Store(&prune.ArchiveMode)
			api._blockReader = reader
			api._txNumReader = reader.TxnumReader()
			contract := common.HexToAddress("0xc0ffee30")
			// Return BLOCKHASH(NUMBER - 1), the parent of the selected block.
			code := hexutil.Bytes{0x60, 0x01, 0x43, 0x03, 0x40, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
			config := &tracersConfig.TraceConfig{
				Tracer:         new("callTracer"),
				TxIndex:        new(hexutil.Uint(0)),
				StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(contract): {Code: &code}},
			}
			block := rpc.BlockNumberOrHashWithHash(target.Hash(), true)
			args := ethapi.CallArgs{From: &m.Address, To: &contract, Gas: new(hexutil.Uint64(100_000))}
			handOff()
			if endpoint == "traceCall" {
				var trace struct{ Output string }
				readGasTrace(t, &trace, func(stream *jsonstream.Stream) error {
					return api.TraceCall(ctx, args, &block, config, stream)
				})
				require.Equal(t, parent.Hash().Hex(), trace.Output)
			} else {
				var traces [][]struct{ Output string }
				readGasTrace(t, &traces, func(stream *jsonstream.Stream) error {
					return api.TraceCallMany(ctx, []Bundle{{Transactions: []ethapi.CallArgs{args}}}, StateContext{BlockNumber: block, TransactionIndex: new(0)}, config, stream)
				})
				require.Len(t, traces, 1)
				require.Len(t, traces[0], 1)
				require.Equal(t, parent.Hash().Hex(), traces[0][0].Output)
			}
			require.Equal(t, viewID, tx.ViewID(), "the KV view must remain pinned throughout tracing")
		})
	}
}

func TestCanonicalViewReaderFrozenFallback(t *testing.T) {
	_, tx := mdbxtest.NewTestTx(t)
	viewHash, backendHash := common.Hash{0x11}, common.Hash{0x22}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, viewHash, 12))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, viewHash, 15))
	for _, tc := range []struct {
		name     string
		block    uint64
		frozen   uint64
		observed bool
		want     common.Hash
		wantErr  bool
	}{
		{name: "unknown_range", block: 5, wantErr: true},
		{name: "no_snapshots", block: 5, observed: true},
		{name: "frozen_block", block: 5, frozen: 10, observed: true, want: backendHash},
		{name: "last_frozen_block", block: 10, frozen: 10, observed: true, want: backendHash},
		{name: "above_frozen_range", block: 11, frozen: 10, observed: true},
		{name: "hole_between_rows", block: 14, frozen: 10, observed: true},
		{name: "present_in_view", block: 12, want: viewHash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &canonicalFrozenBackend{frozen: tc.frozen, hash: backendHash}
			if !tc.observed {
				backend.err = errors.New("backend unavailable")
			}
			reader := canonicalViewReader{freezeblocks.NewRemoteBlockReader(backend)}
			hash, found, err := reader.CanonicalHash(t.Context(), tx, tc.block)
			if tc.wantErr {
				require.ErrorContains(t, err, "frozen block range is unavailable")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, hash)
			require.Equal(t, tc.want != (common.Hash{}), found)
		})
	}
}

type canonicalFrozenBackend struct {
	remoteproto.ETHBACKENDClient
	frozen      uint64
	hash        common.Hash
	err         error
	frozenCalls int
}

func (b *canonicalFrozenBackend) FrozenBlocks(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*remoteproto.FrozenBlocksReply, error) {
	b.frozenCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &remoteproto.FrozenBlocksReply{FrozenBlocks: b.frozen}, b.err
}

func (b *canonicalFrozenBackend) CanonicalHash(context.Context, *remoteproto.CanonicalHashRequest, ...grpc.CallOption) (*remoteproto.CanonicalHashReply, error) {
	return &remoteproto.CanonicalHashReply{Hash: gointerfaces.ConvertHashToH256(b.hash)}, nil
}

func TestCanonicalViewReaderRefreshesFrozenRange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cached uint64
	}{
		{name: "cached_zero"},
		{name: "cached_below_block", cached: 500_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, tx := mdbxtest.NewTestTx(t)
			backend := &canonicalFrozenBackend{frozen: tc.cached, hash: common.Hash{0x77}}
			remote := freezeblocks.NewRemoteBlockReader(backend)
			frozen, observed := remote.FrozenBlocksObserved()
			require.Equal(t, tc.cached, frozen)
			require.True(t, observed)
			backend.frozen = 1_000_000

			reader := canonicalViewReader{remote}
			for range 2 {
				hash, found, err := reader.CanonicalHash(t.Context(), tx, 750_000)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, backend.hash, hash)
			}
			require.Equal(t, 2, backend.frozenCalls, "reuse the refreshed range once it covers the block")
		})
	}
}

func TestCanonicalViewReaderFrozenRangeFailure(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	for _, wantErr := range []error{backendErr, context.Canceled} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			_, tx := mdbxtest.NewTestTx(t)
			backend := &canonicalFrozenBackend{hash: common.Hash{0x77}}
			remote := freezeblocks.NewRemoteBlockReader(backend)
			_, observed := remote.FrozenBlocksObserved()
			require.True(t, observed)
			backend.err = backendErr
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if errors.Is(wantErr, context.Canceled) {
				cancel()
			}

			hash, found, err := (canonicalViewReader{remote}).CanonicalHash(ctx, tx, 750_000)
			require.ErrorIs(t, err, wantErr)
			require.ErrorContains(t, err, "frozen block range is unavailable")
			require.False(t, found)
			require.Zero(t, hash)
		})
	}
}

// Crossing the renewal interval during execution must not mix state from
// different views.
func TestTraceBlockHashDoesNotRenewStateView(t *testing.T) {
	for _, endpoint := range []string{"traceCall", "traceCallMany"} {
		t.Run(endpoint, func(t *testing.T) {
			contract := common.HexToAddress("0xc0ffee31")
			// Return slot 0, BLOCKHASH(NUMBER - 1), and slot 1, in that order.
			code := hexutil.MustDecode("0x600054600052600143034060205260015460405260606000f3")
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: chain.TestChainBerlinConfig,
				Alloc: types.GenesisAlloc{
					testAddr: {Balance: big.NewInt(1_000_000_000_000_000_000)},
					contract: {Code: code, Storage: map[common.Hash]common.Hash{{}: {31: 1}, {31: 1}: {31: 2}}},
				},
			}), execmoduletester.WithKey(testKey))
			blocks, err := m.GenerateChain(2, nil)
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(blocks))
			parent, target := blocks.Blocks[0], blocks.Blocks[1]
			storageSlot := common.Hash{31: 1}
			storageKey := slices.Concat(contract[:], storageSlot[:])

			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				_, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
				reader := freezeblocks.NewRemoteBlockReader(remoteproto.NewETHBACKENDClient(conn))
				db, _ := remoteHistoryDB(t, m.DB)
				tx, handOff := beginRemoteTxForRPC(t, ctx, db)
				viewID := tx.ViewID()
				view := &expireOnStorageReadTx{TemporalTx: tx, expire: func() {
					require.NoError(t, m.DB.UpdateTemporal(ctx, func(rw kv.TemporalRwTx) error {
						maxTxNum, err := rawdbv3.TxNums.Max(ctx, rw, target.NumberU64())
						require.NoError(t, err)
						domains, err := execctx.NewSharedDomains(ctx, rw, m.Log)
						require.NoError(t, err)
						defer domains.Close()
						domains.SetTxNum(maxTxNum + 1)
						require.NoError(t, domains.DomainPut(kv.StorageDomain, rw, storageKey, []byte{9}, maxTxNum+1, nil))
						require.NoError(t, domains.Flush(ctx, rw))
						return rawdb.WriteCanonicalHash(rw, common.Hash{0x42}, parent.NumberU64())
					}))
					time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
				}}
				api := NewPrivateDebugAPI(newBaseApiForTest(m), historyViewDB{TemporalRoDB: db, tx: view}, nil, &rpccfg.DebugApiConfig{})
				api._pruneMode.Store(&prune.ArchiveMode)
				api._blockReader = reader
				api._txNumReader = reader.TxnumReader()
				api.evmCallTimeout = 2 * remotedbserver.MaxTxTTL
				config := &tracersConfig.TraceConfig{Tracer: new("callTracer")}
				block := rpc.BlockNumberOrHashWithHash(target.Hash(), true)
				args := ethapi.CallArgs{From: &testAddr, To: &contract, Gas: new(hexutil.Uint64(100_000))}
				first, second, parentHash := common.Hash{31: 1}, common.Hash{31: 2}, parent.Hash()
				want := slices.Concat(first[:], parentHash[:], second[:])
				handOff()
				if endpoint == "traceCall" {
					var trace struct{ Output string }
					readGasTrace(t, &trace, func(stream *jsonstream.Stream) error {
						return api.TraceCall(ctx, args, &block, config, stream)
					})
					require.Equal(t, hexutil.Encode(want), trace.Output)
				} else {
					var traces [][]struct{ Output string }
					readGasTrace(t, &traces, func(stream *jsonstream.Stream) error {
						return api.TraceCallMany(ctx, []Bundle{{Transactions: []ethapi.CallArgs{args}}}, StateContext{BlockNumber: block}, config, stream)
					})
					require.Len(t, traces, 1)
					require.Len(t, traces[0], 1)
					require.Equal(t, hexutil.Encode(want), traces[0][0].Output)
				}
				require.Nil(t, view.expire, "execution must pass the first storage read")
				require.Equal(t, viewID, tx.ViewID(), "BLOCKHASH must not renew the state view during execution")
				require.NoError(t, m.DB.ViewTemporal(ctx, func(latest kv.TemporalTx) error {
					value, _, err := latest.GetLatest(kv.StorageDomain, storageKey, kv.GetLatestOptions{})
					require.Equal(t, []byte{9}, value, "the backend has newer storage than the trace")
					return err
				}))
			})
		})
	}
}

type expireOnStorageReadTx struct {
	kv.TemporalTx
	expire func()
}

func (tx *expireOnStorageReadTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	value, step, err := tx.TemporalTx.GetLatest(domain, key, opts)
	if domain == kv.StorageDomain && err == nil && tx.expire != nil {
		expire := tx.expire
		tx.expire = nil
		expire()
	}
	return value, step, err
}

func TestIndexedTraceRevalidatesAfterRemoteRenewal(t *testing.T) {
	for _, endpoint := range []string{"traceCall", "traceCallMany"} {
		t.Run(endpoint, func(t *testing.T) {
			apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
			apis.debug._txNumReader = rawdbv3.TxNums
			ref := chainInfo.old
			trace := func(stream *jsonstream.Stream) error {
				bundles, simulate := pruneGatingBundle(ref.num)
				config := &tracersConfig.TraceConfig{Tracer: new("callTracer"), TxIndex: new(hexutil.Uint(1))}
				if endpoint == "traceCall" {
					return apis.debug.TraceCall(t.Context(), bundles[0].Transactions[0], &simulate.BlockNumber, config, stream)
				}
				simulate.TransactionIndex = new(1)
				return apis.debug.TraceCallMany(t.Context(), bundles, simulate, config, stream)
			}
			require.NoError(t, trace(jsonstream.New(new(bytes.Buffer))))

			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				db, _ := remoteHistoryDB(t, apis.debug.db)
				tx, handOff := beginRemoteTxForRPC(t, ctx, db)
				viewID := tx.ViewID()
				minTxNum, err := rawdbv3.TxNums.Min(ctx, tx, ref.num)
				require.NoError(t, err)
				header := rawdb.ReadHeader(tx, ref.hash, ref.num)
				require.NotNil(t, header)
				header.Extra = []byte("replacement")
				view := &renewingStateReadTx{TemporalTx: tx, phase: "pruning", renew: func() {
					require.NoError(t, apis.rwDB.Update(ctx, func(tx kv.RwTx) error {
						if err := rawdb.WriteHeader(tx, header); err != nil {
							return err
						}
						if err := rawdb.WriteCanonicalHash(tx, header.Hash(), ref.num); err != nil {
							return err
						}
						// The replacement has no user transactions, so index 1 is no longer valid.
						return tx.Put(kv.MaxTxNum, hexutil.EncodeTs(ref.num), hexutil.EncodeTs(minTxNum+1))
					}))
					time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
				}}
				apis.debug.db = historyViewDB{TemporalRoDB: db, tx: view}
				var output bytes.Buffer
				stream := jsonstream.New(&output)
				handOff()
				err = trace(stream)
				require.Nil(t, view.renew, "the pruning check must reach the renewal point")
				require.NotEqual(t, viewID, tx.ViewID())
				var invalidParams *rpc.InvalidParamsError
				require.ErrorAs(t, err, &invalidParams)
				require.Empty(t, output.Bytes(), "validation must finish before any trace output")
				require.Empty(t, stream.Buffer())
			})
		})
	}
}

func TestIndexedStateReadFollowsRemoteRenewal(t *testing.T) {
	for _, endpoint := range []string{"account", "storage", "traceCall", "traceCallMany"} {
		for _, phase := range []string{"bounds", "close", "pruning", "bounds_error"} {
			t.Run(endpoint+"/"+phase, func(t *testing.T) {
				apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
				apis.debug._txNumReader = rawdbv3.TxNums
				ref := chainInfo.old
				read := func(ctx context.Context) (any, error) {
					switch endpoint {
					case "account":
						return apis.debug.AccountAt(ctx, ref.hash, 0, testAddr)
					case "storage":
						return apis.debug.StorageRangeAt(ctx, ref.hash, 0, testAddr, nil, 10)
					}
					bundles, simulate := pruneGatingBundle(ref.num)
					simulate.BlockNumber = rpc.BlockNumberOrHashWithHash(ref.hash, true)
					simulate.TransactionIndex = new(0)
					return streamedResult(func(stream *jsonstream.Stream) error {
						if endpoint == "traceCall" {
							return apis.debug.TraceCall(ctx, bundles[0].Transactions[0], &simulate.BlockNumber, &tracersConfig.TraceConfig{TxIndex: new(hexutil.Uint(0))}, stream)
						}
						return apis.debug.TraceCallMany(ctx, bundles, simulate, nil, stream)
					})
				}
				_, err := read(t.Context())
				require.NoError(t, err)

				synctest.Test(t, func(t *testing.T) {
					ctx := t.Context()
					db, _ := remoteHistoryDB(t, apis.debug.db)
					tx, handOff := beginRemoteTxForRPC(t, ctx, db)
					oldViewID := tx.ViewID()
					minTxNum, err := rawdbv3.TxNums.Min(ctx, tx, ref.num)
					require.NoError(t, err)
					view := &renewingStateReadTx{TemporalTx: tx, phase: phase, renew: func() {
						require.NoError(t, apis.rwDB.Update(ctx, func(tx kv.RwTx) error {
							if phase == "bounds_error" {
								// A shorter replacement chain has valid bounds, but combining
								// its maximum with the old minimum produces an empty interval.
								require.Greater(t, minTxNum, uint64(2))
								if err := tx.Put(kv.MaxTxNum, hexutil.EncodeTs(ref.num-1), hexutil.EncodeTs(minTxNum-2)); err != nil {
									return err
								}
								if err := tx.Put(kv.MaxTxNum, hexutil.EncodeTs(ref.num), hexutil.EncodeTs(minTxNum)); err != nil {
									return err
								}
							}
							return rawdb.WriteCanonicalHash(tx, common.Hash{1}, ref.num)
						}))
						time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
					}}
					apis.debug.db = historyViewDB{TemporalRoDB: db, tx: view}
					handOff()
					_, err = read(ctx)
					require.Nil(t, view.renew, "the requested renewal point must be reached")
					require.NotEqual(t, oldViewID, tx.ViewID())
					require.ErrorContains(t, err, "canonical", "a renewed read must recheck the requested block hash")
				})
			})
		}
	}
}

type renewingStateReadTx struct {
	kv.TemporalTx
	phase        string
	renew        func()
	boundsClosed bool
}

func (tx *renewingStateReadTx) trigger(phase string) {
	if tx.phase == phase && tx.renew != nil {
		renew := tx.renew
		tx.renew = nil
		renew()
	}
}

func (tx *renewingStateReadTx) Cursor(bucket string) (kv.Cursor, error) {
	c, err := tx.TemporalTx.Cursor(bucket) //nolint:gocritic // Ownership passes to the caller.
	if err != nil || bucket != kv.MaxTxNum {
		return c, err
	}
	return &renewingBoundsCursor{Cursor: c, tx: tx}, nil
}

func (tx *renewingStateReadTx) GetOne(bucket string, key []byte) ([]byte, error) {
	if tx.boundsClosed && bucket == kv.LastForkchoice {
		tx.trigger("pruning")
	}
	return tx.TemporalTx.GetOne(bucket, key)
}

type renewingBoundsCursor struct {
	kv.Cursor
	tx    *renewingStateReadTx
	seeks int
}

func (c *renewingBoundsCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	if c.seeks == 1 {
		c.tx.trigger("bounds")
		c.tx.trigger("bounds_error")
	}
	c.seeks++
	return c.Cursor.SeekExact(key)
}

func (c *renewingBoundsCursor) Close() {
	c.tx.trigger("close")
	c.Cursor.Close()
	c.tx.boundsClosed = true
}
