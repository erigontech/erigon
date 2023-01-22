package commands

import (
	"context"
	"encoding/json"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

func TestEmptyQuery(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)

	api := NewTraceAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB, &httpcfg.HttpCfg{})
	// Call GetTransactionReceipt for transaction which is not in the database
	var latest = rpc.LatestBlockNumber
	results, err := api.CallMany(context.Background(), json.RawMessage("[]"), &rpc.BlockNumberOrHash{BlockNumber: &latest})
	if err != nil {
		t.Errorf("calling CallMany: %v", err)
	}
	if results == nil {
		t.Errorf("expected empty array, got nil")
	}
	if len(results) > 0 {
		t.Errorf("expected empty array, got %d elements", len(results))
	}
}
func TestCoinbaseBalance(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)

	api := NewTraceAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB, &httpcfg.HttpCfg{})
	// Call GetTransactionReceipt for transaction which is not in the database
	var latest = rpc.LatestBlockNumber
	results, err := api.CallMany(context.Background(), json.RawMessage(`
[
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e","gas":"0x15f90","gasPrice":"0x4a817c800","value":"0x1"},["trace", "stateDiff"]],
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e","gas":"0x15f90","gasPrice":"0x4a817c800","value":"0x1"},["trace", "stateDiff"]]
]
`), &rpc.BlockNumberOrHash{BlockNumber: &latest})
	if err != nil {
		t.Errorf("calling CallMany: %v", err)
	}
	if results == nil {
		t.Errorf("expected empty array, got nil")
	}
	if len(results) != 2 {
		t.Errorf("expected array with 2 elements, got %d elements", len(results))
	}
	// Expect balance increase of the coinbase (zero address)
	if _, ok := results[1].StateDiff[libcommon.Address{}]; !ok {
		t.Errorf("expected balance increase for coinbase (zero address)")
	}
}

func TestReplayTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)

	api := NewTraceAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB, &httpcfg.HttpCfg{})
	var txnHash libcommon.Hash
	if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
		b, err := rawdb.ReadBlockByNumber(tx, 6)
		if err != nil {
			return err
		}
		txnHash = b.Transactions()[5].Hash()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Call GetTransactionReceipt for transaction which is not in the database
	results, err := api.ReplayTransaction(context.Background(), txnHash, []string{"stateDiff"})
	if err != nil {
		t.Errorf("calling ReplayTransaction: %v", err)
	}
	require.NotNil(t, results)
	require.NotNil(t, results.StateDiff)
	addrDiff := results.StateDiff[libcommon.HexToAddress("0x0000000000000006000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.Big)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}

func TestReplayBlockTransactions(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewTraceAPI(NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine), m.DB, &httpcfg.HttpCfg{})

	// Call GetTransactionReceipt for transaction which is not in the database
	n := rpc.BlockNumber(6)
	results, err := api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"stateDiff"})
	if err != nil {
		t.Errorf("calling ReplayBlockTransactions: %v", err)
	}
	require.NotNil(t, results)
	require.NotNil(t, results[0].StateDiff)
	addrDiff := results[0].StateDiff[libcommon.HexToAddress("0x0000000000000001000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.Big)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}
