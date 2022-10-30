package commands

import (
	"context"
	"log"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/assert"
)

func TestErigonGetLatestLogs(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	db := m.DB
	agg := m.HistoryV3Components()
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout), db, nil)
	logs, err := api.GetLatestLogs(context.Background(), filters.FilterCriteria{}, 1)
	if err != nil {
		t.Errorf("get erigon latest logs failed: %v", err)
		log.Fatal(err.Error())
	}
	assert.Equal(0, len(logs))
}
