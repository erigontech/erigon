package commands

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

func TestNewFilters(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, stages.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, nil, nil, mining, func() {})
	api := NewEthAPI(NewBaseApi(ff, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB, nil, nil, nil, 5000000)

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
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, stages.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, nil, nil, mining, func() {})

	// generate some random topics
	topics := make([][]common.Hash, 0)
	for i := 0; i < 10; i++ {
		bytes := make([]byte, length.Hash)
		rand.Read(bytes)
		toAdd := []common.Hash{common.BytesToHash(bytes)}
		topics = append(topics, toAdd)
	}

	// generate some addresses
	addresses := make([]common.Address, 0)
	for i := 0; i < 10; i++ {
		bytes := make([]byte, common.AddressLength)
		rand.Read(bytes)
		addresses = append(addresses, common.BytesToAddress(bytes))
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
