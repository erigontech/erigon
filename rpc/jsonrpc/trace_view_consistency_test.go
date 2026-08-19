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
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

var (
	errUnexpectedOverlayBlockRead = errors.New("unexpected overlay block read")
	errUnexpectedStateCacheView   = errors.New("unexpected state cache view")
)

type rejectOverlayBlockReader struct {
	dbservices.FullBlockReader
}

func (r rejectOverlayBlockReader) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockNum uint64) (*types.Block, []common.Address, error) {
	if view, ok := tx.(interface{ IsOverlayReadView() bool }); blockNum != 0 && ok && view.IsOverlayReadView() {
		return nil, nil, errUnexpectedOverlayBlockRead
	}
	return r.FullBlockReader.BlockWithSenders(ctx, tx, hash, blockNum)
}

type rejectStateCache struct {
	kvcache.Cache
}

func (rejectStateCache) View(context.Context, kv.TemporalTx) (kvcache.CacheView, error) {
	return nil, errUnexpectedStateCacheView
}

func writeNonCanonicalTestBlock(t *testing.T, m *execmoduletester.ExecModuleTester) *types.Header {
	t.Helper()

	var canonicalHeader *types.Header
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		var err error
		canonicalHeader, err = m.BlockReader.HeaderByNumber(m.Ctx, tx, overlayRaceChainSize)
		return err
	}))
	require.NotNil(t, canonicalHeader)

	sideHeader := types.CopyHeader(canonicalHeader)
	sideHeader.Coinbase = common.Address{2}
	require.NotEqual(t, canonicalHeader.Hash(), sideHeader.Hash())
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteHeader(tx, sideHeader); err != nil {
			return err
		}
		return rawdb.WriteBody(tx, sideHeader.Hash(), sideHeader.Number.Uint64(), &types.Body{})
	}))
	return sideHeader
}

func TestTraceCallUsesCommittedState(t *testing.T) {
	m, bankAddress, contractAddress, _ := chainWithDeployedContract(t)

	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	publishedDomains, err := execctx.NewSharedDomains(m.Ctx, roTx, m.Log)
	require.NoError(t, err)
	defer publishedDomains.Close()

	storageKey := common.Hash{}
	compositeKey := make([]byte, 0, len(contractAddress)+len(storageKey))
	compositeKey = append(compositeKey, contractAddress[:]...)
	compositeKey = append(compositeKey, storageKey[:]...)
	require.NoError(t, publishedDomains.DomainPut(kv.StorageDomain, roTx, compositeKey, []byte{3}, 1, nil))

	stateCache := &execmodule.Cache{}
	stateCache.SetPublishedSD(func() *execctx.SharedDomains { return publishedDomains })
	base := newBaseApiForTest(m)
	base.stateCache = stateCache
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	input := hexutil.Bytes(crypto.Keccak256([]byte("retrieve()"))[:4])
	result, err := api.Call(m.Ctx, TraceCallParam{
		From: &bankAddress,
		To:   &contractAddress,
		Data: input,
	}, []string{TraceTypeTrace}, &latest, nil)
	require.NoError(t, err)

	expected := make(hexutil.Bytes, 32)
	expected[len(expected)-1] = 2
	require.Equal(t, expected, result.Output)
}

func TestTraceCallUsesCommittedHeader(t *testing.T) {
	base, m, _, events := newOverlayAheadTestAPIWithEvents(t)

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	committedHeader, err := m.BlockReader.HeaderByNumber(m.Ctx, tx, overlayRaceChainSize)
	require.NoError(t, err)
	require.NotNil(t, committedHeader)
	committedHash := committedHeader.Hash()

	overlayHeader := types.CopyHeader(committedHeader)
	overlayHeader.Coinbase = common.Address{2}
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, rawdb.WriteHeader(overlay, overlayHeader))
	require.NoError(t, rawdb.WriteCanonicalHash(overlay, overlayHeader.Hash(), overlayRaceChainSize))

	contractAddress := common.Address{3}
	coinbaseCode := hexutil.Bytes{byte(vm.COINBASE), 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	traceConfig := &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(contractAddress): {Code: &coinbaseCode},
		},
	}
	requestedBlock := rpc.BlockNumberOrHashWithHash(committedHash, true)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
	result, err := api.Call(m.Ctx, TraceCallParam{
		From: &m.Address,
		To:   &contractAddress,
	}, []string{TraceTypeTrace}, &requestedBlock, traceConfig)
	require.NoError(t, err)

	expected := make(hexutil.Bytes, 32)
	copy(expected[len(expected)-len(committedHeader.Coinbase):], committedHeader.Coinbase[:])
	require.Equal(t, expected, result.Output)
}

func TestAdHocTracesRejectNonCanonicalBlockHash(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	sideHeader := writeNonCanonicalTestBlock(t, m)

	selector := rpc.BlockNumberOrHashWithHash(sideHeader.Hash(), false)
	traceAPI := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
	debugAPI := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	t.Run("trace_call", func(t *testing.T) {
		_, err := traceAPI.Call(m.Ctx, TraceCallParam{}, []string{TraceTypeTrace}, &selector, nil)
		require.ErrorContains(t, err, "is not currently canonical")
	})

	t.Run("trace_callMany", func(t *testing.T) {
		_, err := traceAPI.CallMany(m.Ctx, json.RawMessage("[]"), &selector, nil)
		require.ErrorContains(t, err, "is not currently canonical")
	})

	t.Run("debug_traceCallMany", func(t *testing.T) {
		to := common.Address{3}
		gas := hexutil.Uint64(21_000)
		err := debugAPI.TraceCallMany(m.Ctx, []Bundle{{
			Transactions: []ethapi.CallArgs{{From: &m.Address, To: &to, Gas: &gas}},
		}}, StateContext{BlockNumber: selector}, nil, jsonstream.New(io.Discard))
		require.ErrorContains(t, err, "is not currently canonical")
	})
}

func TestTraceFilterRejectsNonCanonicalBlockHash(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	sideHeader := writeNonCanonicalTestBlock(t, m)
	selector := rpc.BlockNumberOrHashWithHash(sideHeader.Hash(), false)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	t.Run("fromBlock", func(t *testing.T) {
		err := api.Filter(m.Ctx, TraceFilterRequest{FromBlock: &selector}, nil, nil, jsonstream.New(io.Discard))
		require.ErrorContains(t, err, "is not currently canonical")
	})

	t.Run("toBlock", func(t *testing.T) {
		err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &selector}, nil, nil, jsonstream.New(io.Discard))
		require.ErrorContains(t, err, "is not currently canonical")
	})
}

func TestTraceBlockUsesCommittedBlockBody(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._blockReader = rejectOverlayBlockReader{FullBlockReader: base._blockReader}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	result, err := api.Block(m.Ctx, rpc.LatestBlockNumber, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestReplayBlockTransactionsUsesCommittedBlockBody(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._blockReader = rejectOverlayBlockReader{FullBlockReader: base._blockReader}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	result, err := api.ReplayBlockTransactions(m.Ctx, latest, []string{TraceTypeTrace}, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestDebugTraceBlockUsesCommittedBlockBody(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._blockReader = rejectOverlayBlockReader{FullBlockReader: base._blockReader}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	err := api.TraceBlockByNumber(m.Ctx, rpc.LatestBlockNumber, nil, jsonstream.New(io.Discard))

	require.NoError(t, err)
}

func TestSimulateV1UsesCommittedBlockBody(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._blockReader = rejectOverlayBlockReader{FullBlockReader: base._blockReader}
	api := newEthApiForTest(base, m.DB, nil, nil)
	request := SimulationRequest{BlockStateCalls: []SimulatedBlock{{}}}

	result, err := api.SimulateV1(m.Ctx, request, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))

	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestSimulateV1RejectsNonCanonicalBlockHash(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	sideHeader := writeNonCanonicalTestBlock(t, m)

	api := newEthApiForTest(base, m.DB, nil, nil)
	request := SimulationRequest{BlockStateCalls: []SimulatedBlock{{}}}
	selector := rpc.BlockNumberOrHashWithHash(sideHeader.Hash(), false)

	_, err := api.SimulateV1(m.Ctx, request, selector)
	require.ErrorContains(t, err, "is not currently canonical")
}

func TestSimulateV1IgnoresNewerSharedBranchCache(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m, _, _, _ := chainWithDeployedContract(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	request := SimulationRequest{BlockStateCalls: []SimulatedBlock{{}}}
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	baseline, err := api.SimulateV1(m.Ctx, request, latest)
	require.NoError(t, err)
	require.Len(t, baseline, 1)
	expectedRoot, ok := baseline[0]["stateRoot"].(common.Hash)
	require.True(t, ok)

	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	provider, ok := roTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	require.NotNil(t, branchCache)
	branchCache.Clear()
	t.Cleanup(branchCache.Clear)

	_, snapshotTxNum, err := rawdbv3.TxNums.Last(roTx)
	require.NoError(t, err)
	rootKey := nibbles.HexToCompact(nil)
	newerRootBranch := make(commitment.BranchData, 4)
	branchCache.Put(rootKey, newerRootBranch, 0, snapshotTxNum+1)
	cachedRootBranch, _, ok := branchCache.Get(rootKey)
	require.True(t, ok)
	require.Equal(t, []byte(newerRootBranch), cachedRootBranch)

	result, err := api.SimulateV1(m.Ctx, request, latest)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, expectedRoot, result[0]["stateRoot"])
}

func TestExecutionWitnessRejectsNonCanonicalBlockHash(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	sideHeader := writeNonCanonicalTestBlock(t, m)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	selector := rpc.BlockNumberOrHashWithHash(sideHeader.Hash(), false)
	info, err := api.resolveWitnessBlock(m.Ctx, tx, selector)
	require.ErrorContains(t, err, "is not currently canonical")
	require.Nil(t, info)
}

func TestCommittedStateMethodsRejectPendingTag(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	api := newEthApiForTest(base, m.DB, nil, nil)
	debugAPI := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)

	t.Run("eth_simulateV1", func(t *testing.T) {
		_, err := api.SimulateV1(m.Ctx, SimulationRequest{
			BlockStateCalls: []SimulatedBlock{{}},
		}, pending)
		require.EqualError(t, err, "pending state is not supported")
	})

	t.Run("eth_getWitness", func(t *testing.T) {
		_, err := api.GetWitness(m.Ctx, pending)
		require.EqualError(t, err, "pending state is not supported")
	})

	t.Run("eth_getTxWitness", func(t *testing.T) {
		_, err := api.GetTxWitness(m.Ctx, pending, 0)
		require.EqualError(t, err, "pending state is not supported")
	})

	t.Run("eth_getProof", func(t *testing.T) {
		_, err := api.GetProof(m.Ctx, m.Address, nil, &pending)
		require.EqualError(t, err, "pending state is not supported")
	})

	t.Run("debug_executionWitness", func(t *testing.T) {
		_, err := debugAPI.ExecutionWitness(m.Ctx, pending, nil)
		require.EqualError(t, err, "pending state is not supported")
	})
}

func TestExecutionWitnessCacheUsesCommittedView(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	var committedHash common.Hash
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		var ok bool
		var err error
		committedHash, ok, err = m.BlockReader.CanonicalHash(m.Ctx, tx, overlayRaceChainSize)
		require.True(t, ok)
		return err
	}))

	want := &ExecutionWitnessResult{State: []hexutil.Bytes{{1}}}
	api.witnessCache = newWitnessResultCache(96, 0, true, false)
	api.witnessCache.Add(committedHash, want)

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	got, hit, reorgedAway := api.serveFromWitnessCache(m.Ctx, tx, latest, witnessModeLegacy)
	require.True(t, hit)
	require.False(t, reorgedAway)
	require.Same(t, want, got)
}

func TestTraceTransactionUsesCommittedTxnLookup(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._txnReader = rejectOverlayTxnReader{TxnReader: base._txnReader}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	result, err := api.Transaction(m.Ctx, common.Hash{1}, nil, nil)

	require.NoError(t, err)
	require.Nil(t, result)
}

func TestDebugTraceTransactionUsesCommittedTxnLookup(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._txnReader = rejectOverlayTxnReader{TxnReader: base._txnReader}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	err := api.TraceTransaction(m.Ctx, common.Hash{1}, nil, jsonstream.New(io.Discard))

	require.EqualError(t, err, "transaction not found")
}

func TestDebugTraceTransactionUsesCommittedBlockBody(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	minTxNum, err := base._txNumReader.Min(m.Ctx, tx, overlayRaceChainSize)
	require.NoError(t, err)

	base._txnReader = staticTxnReader{
		TxnReader:   base._txnReader,
		blockNumber: overlayRaceChainSize,
		txNum:       minTxNum + 1,
	}
	base._blockReader = rejectOverlayBlockReader{FullBlockReader: base._blockReader}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	err = api.TraceTransaction(m.Ctx, common.Hash{1}, nil, jsonstream.New(io.Discard))

	require.ErrorContains(t, err, "not found")
}

func TestTraceBlockUsesUncachedCommittedState(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base.stateCache = rejectStateCache{}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	_, err := api.Block(m.Ctx, rpc.LatestBlockNumber, nil, nil)

	require.NoError(t, err)
}

func TestDebugTraceBlockUsesUncachedCommittedState(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base.stateCache = rejectStateCache{}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	err := api.TraceBlockByNumber(m.Ctx, rpc.LatestBlockNumber, nil, jsonstream.New(io.Discard))

	require.NoError(t, err)
}

func TestReplayTransactionUsesUncachedCommittedState(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	base.stateCache = rejectStateCache{}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	result, err := api.ReplayTransaction(m.Ctx, common.HexToHash(debugTraceTransactionTests[0].txHash), []string{TraceTypeTrace}, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestDebugTraceTransactionUsesUncachedCommittedState(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	base.stateCache = rejectStateCache{}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	err := api.TraceTransaction(m.Ctx, common.HexToHash(debugTraceTransactionTests[0].txHash), nil, jsonstream.New(io.Discard))

	require.NoError(t, err)
}

func TestGetWitnessUsesCommittedBlockBody(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	base, m, _ := newOverlayAheadTestAPI(t)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	base._blockReader = rejectOverlayBlockReader{FullBlockReader: base._blockReader}
	api := newEthApiForTest(base, m.DB, nil, nil)

	result, err := api.GetWitness(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))

	require.NoError(t, err)
	require.NotNil(t, result)
}
