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

package jsonrpc

import (
	"errors"
	"math/big"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func newBaseApiWithFiltersForTest(f *rpchelper.Filters, stateCache *kvcache.Coherent, m *execmoduletester.ExecModuleTester) *BaseAPI {
	return NewBaseApi(f, stateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
}

func TestLogFilterEndpointsRejectTooManyTopicPositions(t *testing.T) {
	filterManager := rpchelper.New(t.Context(), rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, log.New(), nil)
	api := &APIImpl{
		BaseAPI:                  &BaseAPI{filters: filterManager},
		SubscribeLogsChannelSize: 1,
	}
	criteria := filters.FilterCriteria{Topics: make([][]common.Hash, filters.MaxTopicPositions+1)}

	closeNotifications := make(chan any)
	t.Cleanup(func() { close(closeNotifications) })
	subscriptionContext := rpc.ContextWithNotifier(t.Context(), rpc.NewLocalNotifier("eth", make(chan any, 1), closeNotifications))

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "eth_getLogs",
			call: func() error {
				_, err := api.GetLogs(t.Context(), criteria)
				return err
			},
		},
		{
			name: "eth_newFilter",
			call: func() error {
				_, err := api.NewFilter(t.Context(), criteria)
				return err
			},
		},
		{
			name: `eth_subscribe("logs")`,
			call: func() error {
				_, err := api.Logs(subscriptionContext, criteria)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
			require.EqualError(t, err, "query exceeds the maximum of 4 topics")
		})
	}
}

func TestSubscriptionsRequireFiltersAndNotifier(t *testing.T) {
	m := execmoduletester.New(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	apis := map[string]*APIImpl{
		"withFilters": newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil),
		"nilFilters":  newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil),
	}
	for apiName, api := range apis {
		// ctx carries no rpc notifier, so every subscription method must refuse
		subscriptions := map[string]func() (*rpc.Subscription, error){
			"newHeads":                       func() (*rpc.Subscription, error) { return api.NewHeads(ctx) },
			"newPendingTransactions":         func() (*rpc.Subscription, error) { return api.NewPendingTransactions(ctx, nil) },
			"newPendingTransactionsWithBody": func() (*rpc.Subscription, error) { return api.NewPendingTransactionsWithBody(ctx) },
			"logs":                           func() (*rpc.Subscription, error) { return api.Logs(ctx, filters.FilterCriteria{}) },
			"transactionReceipts": func() (*rpc.Subscription, error) {
				return api.TransactionReceipts(ctx, filters.ReceiptsFilterCriteria{})
			},
		}
		for name, subscribe := range subscriptions {
			sub, err := subscribe()
			require.ErrorIs(t, err, rpc.ErrNotificationsUnsupported, "%s/%s", apiName, name)
			require.Equal(t, &rpc.Subscription{}, sub, "%s/%s", apiName, name)
		}
	}
}

func TestNewFilters(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, execmoduletester.New(t))
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)

	ptf, err := api.NewPendingTransactionFilter(ctx)
	assert.NoError(err)

	nf, err := api.NewFilter(ctx, filters.FilterCriteria{})
	assert.NoError(err)

	bf, err := api.NewBlockFilter(ctx)
	assert.NoError(err)

	ok, err := api.UninstallFilter(ctx, nf)
	assert.NoError(err)
	assert.True(ok)

	ok, err = api.UninstallFilter(ctx, bf)
	assert.NoError(err)
	assert.True(ok)

	ok, err = api.UninstallFilter(ctx, ptf)
	assert.NoError(err)
	assert.True(ok)
}

func TestGetFilterLogsReturnsHistoricalLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(10), ToBlock: big.NewInt(10)}

	expected, err := api.GetLogs(ctx, crit)
	require.NoError(t, err)
	require.NotEmpty(t, expected)

	filterID, err := api.NewFilter(ctx, crit)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = api.UninstallFilter(ctx, filterID)
	})

	for range 2 {
		actual, err := api.GetFilterLogs(ctx, filterID)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}

func TestGetFilterLogsDoesNotConsumeFilterChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)
	crit := filters.FilterCriteria{FromBlock: big.NewInt(10), ToBlock: big.NewInt(10)}

	filterID, err := api.NewFilter(ctx, crit)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = api.UninstallFilter(ctx, filterID)
	})

	queued := &types.RPCLog{
		Log:            types.Log{Address: common.Address{1}},
		BlockTimestamp: 123,
	}
	ff.AddLogs(rpchelper.LogsSubID(strings.TrimPrefix(filterID, "0x")), queued)

	_, err = api.GetFilterLogs(ctx, filterID)
	require.NoError(t, err)
	changes, err := api.GetFilterChanges(ctx, filterID)
	require.NoError(t, err)
	require.Equal(t, []any{queued}, changes)
}

func TestGetFilterLogsReturnsInvalidParamsWhenStoredRangeExceedsLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := NewBaseApi(ff, stateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{
		Dirs:            m.Dirs,
		BlockRangeLimit: 1,
	})
	api := newEthApiForTest(base, m.DB, nil, nil)
	filterID, err := api.NewFilter(ctx, filters.FilterCriteria{FromBlock: big.NewInt(0)})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = api.UninstallFilter(ctx, filterID)
	})

	_, err = api.GetFilterLogs(ctx, filterID)
	require.Error(t, err)
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
	require.Equal(t, errExceedBlockRange+": 1", rpcErr.Error())
}

func TestGetFilterLogsAppliesLogQueryLimitAtPollTime(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	tests := []struct {
		name       string
		criteria   filters.FilterCriteria
		filterConf rpchelper.FiltersConfig
	}{
		{
			name: "addresses",
			criteria: filters.FilterCriteria{
				Addresses: common.Addresses{{1}, {2}},
			},
			filterConf: rpchelper.FiltersConfig{
				RpcSubscriptionFiltersMaxAddresses: 2,
			},
		},
		{
			name: "topic alternatives",
			criteria: filters.FilterCriteria{
				Topics: [][]common.Hash{{{1}, {2}}},
			},
			filterConf: rpchelper.FiltersConfig{
				RpcSubscriptionFiltersMaxTopics: 2,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.filterConf.RpcSubscriptionFiltersTimeout = rpchelper.DefaultFilterTimeout
			ff := rpchelper.New(ctx, test.filterConf, nil, nil, mining, func() {}, m.Log, nil)
			base := NewBaseApi(ff, stateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{
				Dirs:          m.Dirs,
				LogQueryLimit: 1,
			})
			api := newEthApiForTest(base, m.DB, nil, nil)
			test.criteria.FromBlock = big.NewInt(10)
			test.criteria.ToBlock = big.NewInt(10)

			filterID, err := api.NewFilter(ctx, test.criteria)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = api.UninstallFilter(ctx, filterID)
			})

			_, err = api.GetLogs(ctx, test.criteria)
			require.ErrorContains(t, err, "query exceeds the maximum of 1 addresses or topics per search position")

			_, err = api.GetFilterLogs(ctx, filterID)
			require.ErrorContains(t, err, "query exceeds the maximum of 1 addresses or topics per search position")
		})
	}
}

func TestGetFilterLogsDoesNotKeepFilterAlive(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	config := rpchelper.DefaultFiltersConfig
	config.RpcSubscriptionFiltersTimeout = 100 * time.Millisecond
	ff := rpchelper.New(ctx, config, nil, nil, mining, func() {}, m.Log, nil)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)
	filterID, err := api.NewFilter(ctx, filters.FilterCriteria{
		FromBlock: big.NewInt(10),
		ToBlock:   big.NewInt(10),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = api.UninstallFilter(ctx, filterID)
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err = api.GetFilterLogs(ctx, filterID)
		if errors.Is(err, rpc.ErrFilterNotFound) {
			return
		}
		require.NoError(t, err)
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("eth_getFilterLogs kept the filter alive")
}

func TestLogsSubscribeAndUnsubscribe_WithoutConcurrentMapIssue(t *testing.T) {
	ff := rpchelper.New(t.Context(), rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, log.New(), nil)

	// generate some random topics
	topics := make([][]common.Hash, 0, filters.MaxTopicPositions)
	for range filters.MaxTopicPositions {
		bytes := make([]byte, length.Hash)
		rand.Read(bytes)
		toAdd := []common.Hash{common.BytesToHash(bytes)}
		topics = append(topics, toAdd)
	}

	// generate some addresses
	addresses := make([]common.Address, 0, 10)
	for range 10 {
		bytes := make([]byte, length.Addr)
		rand.Read(bytes)
		addresses = append(addresses, common.BytesToAddress(bytes))
	}

	crit := filters.FilterCriteria{
		Topics:    topics,
		Addresses: addresses,
	}

	ids := make([]rpchelper.LogsSubID, 1000)
	errs := make([]error, len(ids))
	unsubscribed := make([]bool, len(ids))

	// make a lot of subscriptions
	wg := sync.WaitGroup{}
	for i := range ids {
		idx := i
		wg.Go(func() {
			_, id, err := ff.SubscribeLogs(32, crit, rpchelper.ProtocolWS)
			ids[idx] = id
			errs[idx] = err
			if err != nil {
				return
			}
			time.Sleep(100 * time.Nanosecond)
			unsubscribed[idx] = ff.UnsubscribeLogs(id)
		})
	}
	wg.Wait()
	for i := range ids {
		require.NoError(t, errs[i])
		require.NotEmpty(t, ids[i])
		require.True(t, unsubscribed[i])
	}
}

func TestBlockFilterGetFilterChangesInitiallyEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	assert := assert.New(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, execmoduletester.New(t))
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)

	// Create a new block filter
	bf, err := api.NewBlockFilter(ctx)
	assert.NoError(err)

	// Immediately query changes; should be empty slice and no error
	changes, err := api.GetFilterChanges(ctx, bf)
	assert.NoError(err)
	assert.Len(changes, 0)

	// Cleanup
	ok, err := api.UninstallFilter(ctx, bf)
	assert.NoError(err)
	assert.True(ok)
}

func TestCompositeFiltersGetFilterChangesInitiallyEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	assert := assert.New(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, execmoduletester.New(t))
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)

	// Create all three filter types
	ptf, err := api.NewPendingTransactionFilter(ctx)
	assert.NoError(err)
	lf, err := api.NewFilter(ctx, filters.FilterCriteria{})
	assert.NoError(err)
	bf, err := api.NewBlockFilter(ctx)
	assert.NoError(err)

	// Immediately query changes on each; expect empty and no error
	changes, err := api.GetFilterChanges(ctx, ptf)
	assert.NoError(err)
	assert.Len(changes, 0)

	changes, err = api.GetFilterChanges(ctx, lf)
	assert.NoError(err)
	assert.Len(changes, 0)

	changes, err = api.GetFilterChanges(ctx, bf)
	assert.NoError(err)
	assert.Len(changes, 0)

	// Cleanup
	ok, err := api.UninstallFilter(ctx, ptf)
	assert.NoError(err)
	assert.True(ok)
	ok, err = api.UninstallFilter(ctx, lf)
	assert.NoError(err)
	assert.True(ok)
	ok, err = api.UninstallFilter(ctx, bf)
	assert.NoError(err)
	assert.True(ok)
}

func TestNewPendingTransactionIncludesFrom(t *testing.T) {
	m := execmoduletester.New(t)
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	tx, err := types.SignTx(
		types.NewTransaction(0, m.Address, uint256.NewInt(1), params.TxGas, uint256.NewInt(1), nil),
		*signer,
		m.Key,
	)
	require.NoError(t, err)

	rpcTx := newRPCPendingTransaction(tx, nil, nil)
	require.Equal(t, m.Address, rpcTx.From)
}

func TestPendingTxsFilterChangesReturnsAllBatches(t *testing.T) {
	m := execmoduletester.New(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)

	ptf, err := api.NewPendingTransactionFilter(ctx)
	require.NoError(t, err)
	id := rpchelper.PendingTxsSubID(strings.TrimPrefix(ptf, "0x"))

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	makeTx := func(nonce uint64) types.Transaction {
		tx, err := types.SignTx(
			types.NewTransaction(nonce, m.Address, uint256.NewInt(1), params.TxGas, uint256.NewInt(1), nil),
			*signer,
			m.Key,
		)
		require.NoError(t, err)
		return tx
	}
	tx0, tx1, tx2 := makeTx(0), makeTx(1), makeTx(2)

	ff.AddPendingTxs(id, []types.Transaction{tx0, tx1})
	ff.AddPendingTxs(id, []types.Transaction{tx2})

	changes, err := api.GetFilterChanges(ctx, ptf)
	require.NoError(t, err)
	require.Equal(t, []any{tx0.Hash(), tx1.Hash(), tx2.Hash()}, changes)

	ok, err := api.UninstallFilter(ctx, ptf)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestGetFilterChangesReturnsFilterNotFoundForUnknownID(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	assert := assert.New(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, execmoduletester.New(t))
	mining := txpoolproto.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)

	// Use a bogus id that does not correspond to any subscription
	_, err := api.GetFilterChanges(ctx, "0xdeadbeefcafebabe")
	assert.ErrorIs(err, rpc.ErrFilterNotFound)
}
