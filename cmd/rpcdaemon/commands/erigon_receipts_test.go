package commands

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErigonGetLatestLogs(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	db := m.DB
	agg := m.HistoryV3Components()
	api := NewErigonAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout), db, nil)
	expectedLogs, _ := api.GetLogs(context.Background(), filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())})

	expectedErigonLogs := make([]*types.ErigonLog, 0)
	for i := len(expectedLogs) - 1; i >= 0; i-- {
		expectedErigonLogs = append(expectedErigonLogs, &types.ErigonLog{
			Address:     expectedLogs[i].Address,
			Topics:      expectedLogs[i].Topics,
			Data:        expectedLogs[i].Data,
			BlockNumber: expectedLogs[i].BlockNumber,
			TxHash:      expectedLogs[i].TxHash,
			TxIndex:     expectedLogs[i].TxIndex,
			BlockHash:   expectedLogs[i].BlockHash,
			Index:       expectedLogs[i].Index,
			Removed:     expectedLogs[i].Removed,
			Timestamp:   expectedLogs[i].Timestamp,
		})
	}
	actual, err := api.GetLatestLogs(context.Background(), filters.FilterCriteria{}, uint64((len(expectedLogs))))
	if err != nil {
		t.Errorf("calling erigon_getLatestLogs: %v", err)
	}
	require.NotNil(t, actual)
	assert.EqualValues(expectedErigonLogs, actual)
}
