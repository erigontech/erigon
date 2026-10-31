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
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
)

// BenchmarkPruneRequests measures API calls with and without the shared history
// floor cache. Remote cases use in-process gRPC transport, without network latency.
func BenchmarkPruneRequests(b *testing.B) {
	m := execmoduletester.New(b, execmoduletester.WithChainConfig(chain.TestChainBerlinConfig))
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	blocks, err := m.GenerateChain(12, func(i int, gen *blockgen.BlockGen) {
		for range 4 {
			txn, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(1), 21000, uint256.NewInt(common.GWei), nil), signer, m.Key)
			require.NoError(b, err)
			gen.AddTx(txn)
		}
	})
	require.NoError(b, err)
	require.NoError(b, m.InsertChain(blocks))
	remoteDB, calls := remoteHistoryDB(b, m.DB)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(b, m)
	client := remoteproto.NewETHBACKENDClient(conn)
	remoteReader := freezeblocks.NewRemoteBlockReader(client)
	backend := rpcservices.NewRemoteBackend(client, remoteDB, remoteReader)
	block := rpc.BlockNumberOrHashWithNumber(8)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(4), ToBlock: big.NewInt(8)}

	for _, transport := range []string{"local", "remote"} {
		for _, cache := range []string{"shared_cache", "no_shared_cache"} {
			var db kv.TemporalRoDB = m.DB
			base := newBaseApiForTest(m)
			if transport == "remote" {
				db = remoteDB
				base = NewBaseApi(nil, m.StateCache, backend, m.Engine, nil)
			}
			if cache == "no_shared_cache" {
				db = noSharedHistoryCacheDB{db}
			}
			base._historyPruneFloor.ttl = time.Hour
			base._blocksPruneFloor.ttl = time.Hour
			api := newEthApiForTest(base, db, nil, nil)
			erigon := NewErigonAPI(base, db, nil)
			for _, request := range []struct {
				name string
				call func() error
			}{
				{"balance", func() error { _, err := api.GetBalance(ctx, m.Address, &block); return err }},
				{"capabilities", func() error { _, err := api.Capabilities(ctx); return err }},
				{"filtered_logs", func() error {
					filter := crit
					filter.Addresses = []common.Address{{2}}
					_, err := api.GetLogs(ctx, filter)
					return err
				}},
				{"latest_logs", func() error {
					_, err := erigon.GetLatestLogs(ctx, crit, filters.LogFilterOptions{LogCount: 1})
					return err
				}},
			} {
				b.Run(transport+"/"+cache+"/"+request.name, func(b *testing.B) {
					require.NoError(b, request.call())
					calls.Store(0)
					b.ReportAllocs()
					for b.Loop() {
						if err := request.call(); err != nil {
							b.Fatal(err)
						}
					}
					if transport == "remote" {
						b.ReportMetric(float64(calls.Load())/float64(b.N), "history_calls/op")
					}
				})
			}
		}
	}
}

// Hiding the optional generation bypasses only the history-floor cache; all
// database and gRPC reads still use the real transaction.
type noSharedHistoryCacheDB struct{ kv.TemporalRoDB }

func (db noSharedHistoryCacheDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic // Ownership passes to the caller.
	if err != nil {
		return nil, err
	}
	return noSharedHistoryCacheTx{tx}, nil
}

type noSharedHistoryCacheTx struct{ kv.TemporalTx }

func (tx noSharedHistoryCacheTx) Debug() kv.TemporalDebugTx {
	return struct{ kv.TemporalDebugTx }{tx.TemporalTx.Debug()}
}

func (tx noSharedHistoryCacheTx) BlockFilesRoTx() *blocksnapshots.View {
	if files, ok := tx.TemporalTx.(freezeblocks.HasBlockFilesRoTx); ok {
		return files.BlockFilesRoTx()
	}
	return nil
}
