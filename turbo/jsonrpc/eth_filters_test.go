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
	"math/rand"
	"sync"
	"testing"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"

	"github.com/erigontech/erigon/rpc/rpccfg"

	"github.com/stretchr/testify/assert"

	txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv/kvcache"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestNewFilters(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mock.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log)
	api := NewEthAPI(NewBaseApi(ff, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil), m.DB, nil, nil, nil, 5000000, 1e18, 100_000, false, 100_000, 128, log.New())

	ptf, err := api.NewPendingTransactionFilter(ctx)
	assert.Nil(err)

	nf, err := api.NewFilter(ctx, filters.FilterCriteria{})
	assert.Nil(err)

	bf, err := api.NewBlockFilter(ctx)
	assert.Nil(err)

	ok, err := api.UninstallFilter(ctx, nf)
	assert.Nil(err)
	assert.Equal(ok, true)

	ok, err = api.UninstallFilter(ctx, bf)
	assert.Nil(err)
	assert.Equal(ok, true)

	ok, err = api.UninstallFilter(ctx, ptf)
	assert.Nil(err)
	assert.Equal(ok, true)
}

func TestLogsSubscribeAndUnsubscribe_WithoutConcurrentMapIssue(t *testing.T) {
	m := mock.Mock(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log)

	// generate some random topics
	topics := make([][]libcommon.Hash, 0)
	for i := 0; i < 10; i++ {
		bytes := make([]byte, length.Hash)
		rand.Read(bytes)
		toAdd := []libcommon.Hash{libcommon.BytesToHash(bytes)}
		topics = append(topics, toAdd)
	}

	// generate some addresses
	addresses := make([]libcommon.Address, 0)
	for i := 0; i < 10; i++ {
		bytes := make([]byte, length.Addr)
		rand.Read(bytes)
		addresses = append(addresses, libcommon.BytesToAddress(bytes))
	}

	crit := filters.FilterCriteria{
		Topics:    topics,
		Addresses: addresses,
	}

	ids := make([]rpchelper.LogsSubID, 1000)

	// make a lot of subscriptions
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(idx int) {
			_, id := ff.SubscribeLogs(32, crit)
			defer func() {
				time.Sleep(100 * time.Nanosecond)
				ff.UnsubscribeLogs(id)
				wg.Done()
			}()
			ids[idx] = id
		}(i)
	}
	wg.Wait()
}
