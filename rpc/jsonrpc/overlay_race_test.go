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
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const (
	overlayRaceChainSize = 5
	overlayRaceBaseFee   = 424242
	overlayRaceLowTip    = 1_000_000
	overlayRaceHighTip   = 2_000_000
)

func insertOverlayRaceChain(t *testing.T, m *execmoduletester.ExecModuleTester) *blockgen.ChainPack {
	t.Helper()
	c, err := m.GenerateChain(overlayRaceChainSize, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))
	return c
}

// writeHeadBlockMarkers writes the minimal subset of what InsertBlocks and
// updateForkChoice persist for a new head — header, canonical marker, body,
// head-header and forkchoice markers. The forkchoice marker is what
// rpchelper.GetLatestBlockNumber resolves the head from, so reader paths under
// test (the gas oracle, "latest" tag resolution) see this block as current.
func writeHeadBlockMarkers(t *testing.T, tx kv.RwTx, header *types.Header, body *types.Body) {
	t.Helper()
	require.NoError(t, writeHeadBlockMarkersE(tx, header, body))
}

// writeHeadBlockMarkersE is the error-returning core, safe to call from hooks
// running on non-test goroutines (testify's FailNow contract).
func writeHeadBlockMarkersE(tx kv.RwTx, header *types.Header, body *types.Body) error {
	hash := header.Hash()
	num := header.Number.Uint64()
	if err := rawdb.WriteHeader(tx, header); err != nil {
		return err
	}
	if err := rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(tx, hash, num); err != nil {
		return err
	}
	if err := rawdb.WriteBody(tx, hash, num, body); err != nil {
		return err
	}
	rawdb.WriteForkchoiceHead(tx, hash)
	return nil
}

type overlayAheadHarness struct {
	t             *testing.T
	base          *BaseAPI
	m             *execmoduletester.ExecModuleTester
	overlayHeader *types.Header
	overlayBody   *types.Body
	events        *shards.Events
	doms          *execctx.SharedDomains
}

// newOverlayAheadHarness builds overlayRaceChainSize committed MDBX blocks,
// then publishes a fabricated block one past them (overlayRaceChainSize+1)
// into the block overlay only, never committed to MDBX. This reproduces the
// window where forkchoice publishes the overlay before the MDBX commit
// lands, so a plain tx would still report the previous head.
//
// The overlay block's GasUsed is set to exactly its EIP-1559 target so
// misc.CalcBaseFee leaves BaseFee unchanged, making overlayRaceBaseFee a
// reliable, deterministic fingerprint for "the code read the overlay head".
// When withOverlayTxs is set, the block instead carries two transactions with
// distinct tips plus their receipt-domain entries, and GasUsed equals the
// transactions' real total so reward percentile thresholds relate to the
// receipts' gas.
func newOverlayAheadHarness(t *testing.T, withOverlayTxs bool) *overlayAheadHarness {
	t.Helper()

	var cfg chain.Config
	require.NoError(t, copier.CopyWithOption(&cfg, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
	cfg.LondonBlock = common.NewUint64(0)
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(&cfg))

	c := insertOverlayRaceChain(t, m)
	base, doms, events, overlayRoTx := newPublishedOverlayTestBase(t, m)

	const overlayGasLimit = 30_000_000
	overlayNumber := uint64(overlayRaceChainSize) + 1

	var overlayTxs types.Transactions
	overlayGasUsed := uint64(overlayGasLimit / params.ElasticityMultiplier) // == target: CalcBaseFee leaves BaseFee unchanged
	if withOverlayTxs {
		overlayTxs = types.Transactions{
			signOverlayRaceTestTxWithTip(t, m, 0, overlayRaceLowTip),
			signOverlayRaceTestTxWithTip(t, m, 1, overlayRaceHighTip),
		}
		overlayGasUsed = uint64(len(overlayTxs)) * params.TxGas
	}

	overlayHeader := &types.Header{
		ParentHash: c.TopBlock.Hash(),
		Number:     *uint256.NewInt(overlayNumber),
		Difficulty: *uint256.NewInt(0),
		Time:       c.TopBlock.Time() + 10,
		GasLimit:   overlayGasLimit,
		GasUsed:    overlayGasUsed,
		BaseFee:    uint256.NewInt(overlayRaceBaseFee),
	}
	hash := overlayHeader.Hash()
	overlayBody := &types.Body{Transactions: overlayTxs}
	overlay := doms.BlockOverlay()
	writeHeadBlockMarkers(t, overlay, overlayHeader, overlayBody)

	if withOverlayTxs {
		senders := slices.Repeat([]common.Address{m.Address}, len(overlayTxs))
		require.NoError(t, rawdb.WriteSenders(overlay, hash, overlayNumber, senders))
		// Receipt-domain entries go through the SharedDomains (like execution writes
		// them), so readers reach them via the view's DomainReader, not the overlay tables.
		minTxNum, err := m.BlockReader.TxnumReader().Min(m.Ctx, overlayRoTx, overlayNumber)
		require.NoError(t, err)
		putDel := doms.AsPutDel(overlayRoTx)
		var cumGas uint64
		for i := range overlayTxs {
			cumGas += params.TxGas
			require.NoError(t, rawtemporaldb.AppendReceiptMetadata(putDel, 0, cumGas, 0, minTxNum+1+uint64(i)))
		}
	}

	return &overlayAheadHarness{t: t, base: base, m: m, overlayHeader: overlayHeader, overlayBody: overlayBody, events: events, doms: doms}
}

// newOverlayAheadTestAPI and newOverlayAheadTestAPIWithEvents expose the
// harness to the tests that only need the plain overlay-ahead block.
func newOverlayAheadTestAPI(t *testing.T) (base *BaseAPI, m *execmoduletester.ExecModuleTester, overlayHeader *types.Header) {
	base, m, overlayHeader, _ = newOverlayAheadTestAPIWithEvents(t)
	return base, m, overlayHeader
}

func newOverlayAheadTestAPIWithEvents(t *testing.T) (*BaseAPI, *execmoduletester.ExecModuleTester, *types.Header, *shards.Events) {
	h := newOverlayAheadHarness(t, false)
	return h.base, h.m, h.overlayHeader, h.events
}

func newPublishedOverlayTestBase(t *testing.T, m *execmoduletester.ExecModuleTester) (*BaseAPI, *execctx.SharedDomains, *shards.Events, kv.TemporalTx) {
	t.Helper()

	overlayRoTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	t.Cleanup(overlayRoTx.Rollback)
	doms, err := execctx.NewSharedDomains(m.Ctx, overlayRoTx, m.Log)
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	require.NoError(t, doms.InitBlockOverlay(overlayRoTx, m.Dirs.Tmp))

	events := shards.NewEvents()
	events.PublishOverlay(doms)
	t.Cleanup(func() { events.PublishOverlay(nil) })
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return newBaseApiWithFiltersForTest(ff, stateCache, m), doms, events, overlayRoTx
}

type unpublishOverlayBlockReader struct {
	dbservices.FullBlockReader
	events      *shards.Events
	blockNumber uint64
}

type rejectOverlayTxnReader struct {
	dbservices.TxnReader
}

type unpublishOverlayTxnReader struct {
	dbservices.TxnReader
	events *shards.Events
}

func (r unpublishOverlayTxnReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, uint64, bool, error) {
	blockNum, txNum, ok, err := r.TxnReader.TxnLookup(ctx, tx, txnHash)
	if err == nil && ok {
		r.events.PublishOverlay(nil)
	}
	return blockNum, txNum, ok, err
}

func (r rejectOverlayTxnReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, uint64, bool, error) {
	if carriesOverlayView(tx) {
		return 0, 0, false, errors.New("unexpected overlay transaction lookup")
	}
	return r.TxnReader.TxnLookup(ctx, tx, txnHash)
}

type staticTxnReader struct {
	dbservices.TxnReader
	blockNumber uint64
	txNum       uint64
}

func (r staticTxnReader) TxnLookup(context.Context, kv.Getter, common.Hash) (uint64, uint64, bool, error) {
	return r.blockNumber, r.txNum, true, nil
}

type hideHeaderBlockReader struct {
	dbservices.FullBlockReader
	blockNumber uint64
}

func (r hideHeaderBlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNumber uint64) (*types.Header, error) {
	if blockNumber == r.blockNumber {
		return nil, nil
	}
	return r.FullBlockReader.Header(ctx, tx, hash, blockNumber)
}

type failHeaderReadBlockReader struct {
	dbservices.FullBlockReader
	hash        common.Hash
	blockNumber uint64
	err         error
}

func (r failHeaderReadBlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockNumber uint64) (*types.Header, error) {
	if hash == r.hash {
		return nil, r.err
	}
	return r.FullBlockReader.Header(ctx, tx, hash, blockNumber)
}

func (r failHeaderReadBlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockNumber uint64) (*types.Header, error) {
	if blockNumber == r.blockNumber {
		return nil, r.err
	}
	return r.FullBlockReader.HeaderByNumber(ctx, tx, blockNumber)
}

type failOverlayHeaderNumberBlockReader struct {
	dbservices.FullBlockReader
	err error
}

func (r failOverlayHeaderNumberBlockReader) HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	if carriesOverlayView(tx) {
		return nil, r.err
	}
	return r.FullBlockReader.HeaderNumber(ctx, tx, hash)
}

type failBlockWithSendersReader struct {
	dbservices.FullBlockReader
	err error
}

func (r failBlockWithSendersReader) BlockWithSenders(context.Context, kv.Getter, common.Hash, uint64) (*types.Block, []common.Address, error) {
	return nil, nil, r.err
}

// carriesOverlayView reports whether a reader was handed a tx pinned to an
// overlay view, the marker the wrap points key on.
func carriesOverlayView(tx kv.Getter) bool {
	c, ok := tx.(membatchwithdb.OverlayViewCarrier)
	if !ok {
		return false
	}
	_, pinned := c.OverlayView()
	return pinned
}

type publishOverlayOnSecondProbeTx struct {
	kv.Tx
	probes  int
	publish func()
}

func (tx *publishOverlayOnSecondProbeTx) OverlayView() (*membatchwithdb.MemoryMutation, bool) {
	tx.probes++
	if tx.probes == 2 {
		tx.publish()
	}
	return nil, false
}

func (r hideHeaderBlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockNumber uint64) (*types.Header, error) {
	if blockNumber == r.blockNumber {
		return nil, nil
	}
	return r.FullBlockReader.HeaderByNumber(ctx, tx, blockNumber)
}

func (r *unpublishOverlayBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (common.Hash, bool, error) {
	hash, ok, err := r.FullBlockReader.CanonicalHash(ctx, tx, blockNum)
	if err == nil && ok && blockNum == r.blockNumber {
		r.events.PublishOverlay(nil)
	}
	return hash, ok, err
}

func newOverlayUnpublishTestAPI(t *testing.T) (*BaseAPI, *execmoduletester.ExecModuleTester, *types.Header) {
	t.Helper()
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	txn := signOverlayRaceTestTx(t, m, 1)
	require.NoError(t, rawdb.WriteBody(overlay, overlayHeader.Hash(), overlayHeader.Number.Uint64(), &types.Body{Transactions: []types.Transaction{txn}}))
	base._blockReader = &unpublishOverlayBlockReader{
		FullBlockReader: base._blockReader,
		events:          events,
		blockNumber:     overlayHeader.Number.Uint64(),
	}
	return base, m, overlayHeader
}

func writeOverlayReorgHeader(t *testing.T, base *BaseAPI, m *execmoduletester.ExecModuleTester) *types.Header {
	t.Helper()

	var canonicalHeader *types.Header
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		var err error
		canonicalHeader, err = m.BlockReader.HeaderByNumber(m.Ctx, tx, overlayRaceChainSize)
		return err
	}))
	require.NotNil(t, canonicalHeader)

	reorgHeader := types.CopyHeader(canonicalHeader)
	reorgHeader.Coinbase = common.Address{2}
	require.NotEqual(t, canonicalHeader.Hash(), reorgHeader.Hash())

	overlay := base.filters.LatestSD().BlockOverlay()
	require.NoError(t, rawdb.WriteHeader(overlay, reorgHeader))
	require.NoError(t, rawdb.WriteCanonicalHash(overlay, reorgHeader.Hash(), reorgHeader.Number.Uint64()))
	require.NoError(t, rawdb.WriteBody(overlay, reorgHeader.Hash(), reorgHeader.Number.Uint64(), &types.Body{}))
	return reorgHeader
}

// overlayRaceTxPoolClient extends stubTxPoolClient with canned replies for
// the single method each test needs.
type overlayRaceTxPoolClient struct {
	stubTxPoolClient
	transactionsReply *txpoolproto.TransactionsReply
	allReply          *txpoolproto.AllReply
}

func (c *overlayRaceTxPoolClient) Transactions(context.Context, *txpoolproto.TransactionsRequest, ...grpc.CallOption) (*txpoolproto.TransactionsReply, error) {
	return c.transactionsReply, nil
}

func (c *overlayRaceTxPoolClient) All(context.Context, *txpoolproto.AllRequest, ...grpc.CallOption) (*txpoolproto.AllReply, error) {
	return c.allReply, nil
}

func signOverlayRaceTestTx(t *testing.T, m *execmoduletester.ExecModuleTester, nonce uint64) types.Transaction {
	t.Helper()
	return signOverlayRaceTestTxWithTip(t, m, nonce, 0)
}

func signOverlayRaceTestTxWithTip(t *testing.T, m *execmoduletester.ExecModuleTester, nonce uint64, tip uint64) types.Transaction {
	t.Helper()
	signer := types.LatestSigner(m.ChainConfig)
	txn, err := types.SignTx(
		types.NewEIP1559Transaction(*m.ChainConfig.ChainID, nonce, common.HexToAddress("deadbeef"), uint256.NewInt(1), 21000, nil, uint256.NewInt(tip), uint256.NewInt(1_000_000_000_000), nil),
		*signer, m.Key,
	)
	require.NoError(t, err)
	return txn
}

func marshalOverlayRaceTestTx(t *testing.T, txn types.Transaction) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&buf))
	return buf.Bytes()
}

func newOverlayTransactionTestData(t *testing.T) (*BaseAPI, *execmoduletester.ExecModuleTester, types.Transaction) {
	t.Helper()
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, overlayHeader.Number.Uint64()))
	txn := signOverlayRaceTestTx(t, m, 0)
	require.NoError(t, rawdb.WriteBody(overlay, overlayHeader.Hash(), overlayHeader.Number.Uint64(), &types.Body{Transactions: []types.Transaction{txn}}))
	minTxNum, err := base._txNumReader.Min(m.Ctx, overlay, overlayHeader.Number.Uint64())
	require.NoError(t, err)
	block := types.NewBlockFromStorage(overlayHeader.Hash(), overlayHeader, []types.Transaction{txn}, nil, nil, nil)
	rawdb.WriteTxLookupEntries(overlay, block, minTxNum)
	base._txnReader = unpublishOverlayTxnReader{TxnReader: base._txnReader, events: events}
	return base, m, txn
}

// TestGetBlockByTimestamp_SeesOverlayHead pins that GetBlockByTimestamp resolves
// "current" through the block overlay: querying a timestamp at or after the
// overlay head must return that in-flight block, not the last MDBX-committed one.
func TestGetBlockByTimestamp_SeesOverlayHead(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := NewErigonAPI(h.base, h.m.DB, nil)

	resp, err := api.GetBlockByTimestamp(h.m.Ctx, rpc.Timestamp(h.overlayHeader.Time), false)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, h.overlayHeader.Number.ToBig(), resp["number"].(*hexutil.U256).ToInt(),
		"must resolve to the overlay head block, not the stale MDBX-committed head")
}

func TestGetModifiedAccountsByNumber_UsesCommittedStartTag(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := NewPrivateDebugAPI(h.base, h.m.DB, nil, &rpccfg.DebugApiConfig{})

	result, err := api.GetModifiedAccountsByNumber(h.m.Ctx, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}

func TestGetModifiedAccountsByNumber_UsesCommittedEndTag(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := NewPrivateDebugAPI(h.base, h.m.DB, nil, &rpccfg.DebugApiConfig{})
	latest := rpc.LatestBlockNumber

	result, err := api.GetModifiedAccountsByNumber(h.m.Ctx, rpc.EarliestBlockNumber, &latest)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}

func TestResolveWitnessBlockUsesCommittedView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	info, err := api.resolveWitnessBlock(m.Ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	require.NoError(t, err)
	require.Equal(t, uint64(overlayRaceChainSize), info.BlockNum)
	require.NotEqual(t, overlayHeader.Hash(), info.Block.Hash())
}

// TestGetTransactionByHash_PendingTx_UsesOverlayHead pins that the pending-tx
// fallback in GetTransactionByHash reads the current header through the block
// overlay: the returned tx's gas price (derived from that header's base fee)
// must reflect the overlay head, not the stale MDBX-committed head.
func TestGetTransactionByHash_PendingTx_UsesOverlayHead(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)

	pendingTxn := signOverlayRaceTestTx(t, h.m, 1)
	pool := &overlayRaceTxPoolClient{
		transactionsReply: &txpoolproto.TransactionsReply{RlpTxs: [][]byte{marshalOverlayRaceTestTx(t, pendingTxn)}},
	}
	api := newEthApiForTest(h.base, h.m.DB, pool, nil)

	got, err := api.GetTransactionByHash(h.m.Ctx, pendingTxn.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
		"pending tx gas price must be derived from the overlay head's base fee, not the stale MDBX head")
}

// TestGetTransactionByBlockNumberAndIndex_PublishCycleDuringTxAcquisition pins
// atomic acquisition for the eth_txs family, whose block resolution and block
// read each resolve the overlay on their own.
func TestGetTransactionByBlockNumberAndIndex_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, true)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := newEthApiForTest(h.base, newCycleHookDB(h, true), nil, nil)

	txn, err := api.GetTransactionByBlockNumberAndIndex(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()), 0)
	require.NoError(t, err)
	require.NotNil(t, txn,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestTransactionByHashMethodsPinOverlayView(t *testing.T) {
	t.Run("transaction", func(t *testing.T) {
		base, m, txn := newOverlayTransactionTestData(t)
		pool := &overlayRaceTxPoolClient{transactionsReply: &txpoolproto.TransactionsReply{RlpTxs: [][]byte{nil}}}
		api := newEthApiForTest(base, m.DB, pool, nil)

		got, err := api.GetTransactionByHash(m.Ctx, txn.Hash())
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, txn.Hash(), got.Hash)
	})

	t.Run("raw transaction", func(t *testing.T) {
		base, m, txn := newOverlayTransactionTestData(t)
		pool := &overlayRaceTxPoolClient{transactionsReply: &txpoolproto.TransactionsReply{RlpTxs: [][]byte{nil}}}
		api := newEthApiForTest(base, m.DB, pool, nil)

		got, err := api.GetRawTransactionByHash(m.Ctx, txn.Hash())
		require.NoError(t, err)
		require.Equal(t, marshalOverlayRaceTestTx(t, txn), []byte(got))
	})
}

func TestGetTransactionReceiptPinsOverlayView(t *testing.T) {
	base, m, txn := newOverlayTransactionTestData(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	receipt, err := api.GetTransactionReceipt(m.Ctx, txn.Hash())
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, txn.Hash(), receipt["transactionHash"])
}

// TestGetBlockReceipts_PublishCycleDuringTxAcquisition pins the same atomic
// acquisition for the receipt family: the block whose receipts are requested is
// the head, so a cycle landing during the open must not make it unavailable.
func TestGetBlockReceipts_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := newEthApiForTest(h.base, db, nil, nil)

	receipts, err := api.GetBlockReceipts(h.m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(h.overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotNil(t, receipts,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestGetTransactionReceiptRejectsMismatchedTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)

	const blockNumber = uint64(6)
	var txNum uint64
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		minTxNum, err := base._txNumReader.Min(m.Ctx, tx, blockNumber)
		if err != nil {
			return err
		}
		txNum = minTxNum + 1
		return nil
	}))

	base._txnReader = staticTxnReader{TxnReader: base._txnReader, blockNumber: blockNumber, txNum: txNum}
	api := newEthApiForTest(base, m.DB, nil, nil)

	receipt, err := api.GetTransactionReceipt(m.Ctx, common.Hash{0xff})
	require.NoError(t, err)
	require.Nil(t, receipt)
}

func TestGetBlockReceiptsPinsOverlayView(t *testing.T) {
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, overlayHeader.Number.Uint64()))
	base._blockReader = &unpublishOverlayBlockReader{
		FullBlockReader: base._blockReader,
		events:          events,
		blockNumber:     overlayHeader.Number.Uint64(),
	}
	api := newEthApiForTest(base, m.DB, nil, nil)

	receipts, err := api.GetBlockReceipts(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotNil(t, receipts)
	require.Empty(t, receipts)
}

// newOverlayRacePendingPool signs a single pending tx from m.Address and wraps
// it in an overlayRaceTxPoolClient.All reply, as txpool_content/contentFrom expect.
func newOverlayRacePendingPool(t *testing.T, m *execmoduletester.ExecModuleTester) (*overlayRaceTxPoolClient, types.Transaction) {
	t.Helper()
	txn := signOverlayRaceTestTx(t, m, 1)
	pool := &overlayRaceTxPoolClient{
		allReply: &txpoolproto.AllReply{Txs: []*txpoolproto.AllReply_Tx{{
			TxnType: txpoolproto.AllReply_PENDING,
			Sender:  gointerfaces.ConvertAddressToH160(m.Address),
			RlpTx:   marshalOverlayRaceTestTx(t, txn),
		}}},
	}
	return pool, txn
}

// TestTxPoolContent_UsesOverlayHead pins that txpool_content reads the current
// header through the block overlay, matching TestGetTransactionByHash_PendingTx_UsesOverlayHead.
func TestTxPoolContent_UsesOverlayHead(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	pool, txn := newOverlayRacePendingPool(t, h.m)
	api := NewTxPoolAPI(h.base, h.m.DB, pool)

	content, err := api.Content(h.m.Ctx)
	require.NoError(t, err)
	got := content["pending"][h.m.Address.Hex()][strconv.FormatUint(txn.GetNonce(), 10)]
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
		"pending tx gas price must be derived from the overlay head's base fee, not the stale MDBX head")
}

// TestTxPoolContent_PublishCycleDuringTxAcquisition pins atomic acquisition for
// the txpool family: the pending gas price is derived from the head base fee,
// so a cycle landing during the open silently prices against the stale head.
func TestTxPoolContent_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	pool, txn := newOverlayRacePendingPool(t, h.m)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := NewTxPoolAPI(h.base, newCycleHookDB(h, true), pool)

	content, err := api.Content(h.m.Ctx)
	require.NoError(t, err)
	got := content["pending"][h.m.Address.Hex()][strconv.FormatUint(txn.GetNonce(), 10)]
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
		"a publish/commit/unpublish cycle during tx acquisition must not price against the stale head")
}

// TestGetBlockTransactionCountByHash_SeesOverlayHead pins that the by-hash
// count resolves the overlay head exactly like its by-number twin: the same
// in-flight block must be visible through both, not null through one of them.
func TestGetBlockTransactionCountByHash_SeesOverlayHead(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	byNumber, err := api.GetBlockTransactionCountByNumber(m.Ctx, rpc.BlockNumber(overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, byNumber)

	byHash, err := api.GetBlockTransactionCountByHash(m.Ctx, overlayHeader.Hash())
	require.NoError(t, err)
	require.NotNil(t, byHash, "by-hash count must see the overlay head the by-number count sees")
	require.Equal(t, *byNumber, *byHash)
}

func TestGetUncleCountByBlockHash_SeesOverlayHead(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	byNumber, err := api.GetUncleCountByBlockNumber(m.Ctx, rpc.BlockNumber(overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, byNumber)

	byHash, err := api.GetUncleCountByBlockHash(m.Ctx, overlayHeader.Hash())
	require.NoError(t, err)
	require.NotNil(t, byHash)
	require.Equal(t, *byNumber, *byHash)
}

// TestGetUncleCountByBlockNumber_PublishCycleDuringTxAcquisition pins the same
// atomic acquisition for the uncle family.
func TestGetUncleCountByBlockNumber_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := newEthApiForTest(h.base, newCycleHookDB(h, true), nil, nil)

	count, err := api.GetUncleCountByBlockNumber(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, count,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestGetBlockTransactionCountByNumber_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	count, err := api.GetBlockTransactionCountByNumber(m.Ctx, rpc.BlockNumber(overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, count)
	require.Equal(t, hexutil.Uint(1), *count)
}

func TestGetBlockTransactionCountByHash_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	count, err := api.GetBlockTransactionCountByHash(m.Ctx, overlayHeader.Hash())
	require.NoError(t, err)
	require.NotNil(t, count)
	require.Equal(t, hexutil.Uint(1), *count)
}

// TestGetBlockByNumber_PublishCycleDuringTxAcquisition pins atomic acquisition
// for the eth_block family. The handler never names the overlay: it is resolved
// inside rpchelper.GetBlockNumber and again inside blockByNumber, so an unpinned
// tx can answer those two from different overlay generations, and a cycle
// landing during the open leaves the head block in neither layer.
func TestGetBlockByNumber_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := newEthApiForTest(h.base, newCycleHookDB(h, true), nil, nil)

	block, err := api.GetBlockByNumber(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()), false)
	require.NoError(t, err)
	require.NotNil(t, block,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

// TestGetBlockByNumber_SiblingPublishDuringTxAcquisition covers the canonical
// change the publish/commit cycle does not: a same-height sibling published
// while the tx is acquired means the capture never stabilizes, so the request
// must answer on the committed head rather than layer a sibling generation
// over a snapshot it was never matched against.
func TestGetBlockByNumber_SiblingPublishDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	db := &beginHookDB{TemporalRoDB: h.m.DB, t: h.t, hook: func() error {
		return publishOverlayHeadE(h, siblingOfOverlayHead(h))
	}}
	api := newEthApiForTest(h.base, db, nil, nil)

	var committed *types.Header
	require.NoError(t, h.m.DB.View(h.m.Ctx, func(tx kv.Tx) error {
		committed = rawdb.ReadHeaderByNumber(tx, overlayRaceChainSize)
		return nil
	}))
	require.NotNil(t, committed)

	got, err := api.GetBlockByNumber(h.m.Ctx, rpc.LatestBlockNumber, false)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, committed.Hash(), got["hash"],
		"an unstable capture must fall back to the committed head, not serve the sibling generation")
}

// newRemoteModeTestAPI builds the rpcdaemon shape that never sees an overlay:
// filters without an events source, where every acquisition pins nil.
func newRemoteModeTestAPI(t *testing.T) (*BaseAPI, *execmoduletester.ExecModuleTester, *types.Header) {
	t.Helper()
	m := execmoduletester.New(t)
	c := insertOverlayRaceChain(t, m)
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, nil)
	base := newBaseApiWithFiltersForTest(ff, kvcache.New(kvcache.DefaultCoherentConfig), m)
	return base, m, c.TopBlock.Header()
}

// TestGetBlockByNumber_RemoteModeServesCommittedHead covers the remote/no-overlay
// mode of the acquisition helper: with no events source the pin is always nil,
// and the handler must keep serving the committed head unchanged.
func TestGetBlockByNumber_RemoteModeServesCommittedHead(t *testing.T) {
	t.Parallel()
	base, m, head := newRemoteModeTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	got, err := api.GetBlockByNumber(m.Ctx, rpc.LatestBlockNumber, false)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, head.Hash(), got["hash"],
		"a nil pin must read committed data exactly as an unwrapped tx does")
}

func TestGetBlockTransactionCountByNumber_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := newEthApiForTest(h.base, newCycleHookDB(h, true), nil, nil)

	count, err := api.GetBlockTransactionCountByNumber(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, count,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestGetBlockTransactionCountByHashReturnsNullWithoutBody(t *testing.T) {
	m, aheadHash := newHeaderAheadTester(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	count, err := api.GetBlockTransactionCountByHash(m.Ctx, aheadHash)
	require.NoError(t, err)
	require.Nil(t, count)
}

func TestGetRawHeader_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	header, err := api.GetRawHeader(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotNil(t, header)
}

func TestGetRawBlock_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	block, err := api.GetRawBlock(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotNil(t, block)
}

func TestGetRawReceipts_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, overlayHeader.Number.Uint64()))
	base._blockReader = &unpublishOverlayBlockReader{
		FullBlockReader: base._blockReader,
		events:          events,
		blockNumber:     overlayHeader.Number.Uint64(),
	}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	receipts, err := api.GetRawReceipts(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotNil(t, receipts)
}

func TestGetRawTransaction_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, txn := newOverlayTransactionTestData(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	encoded, err := api.GetRawTransaction(m.Ctx, txn.Hash())
	require.NoError(t, err)
	require.Equal(t, marshalOverlayRaceTestTx(t, txn), []byte(encoded))
}

// TestGetRawHeader_PublishCycleDuringTxAcquisition pins that the raw debug
// family acquires its tx and its overlay atomically. Resolving the overlay
// after the open lets a publish/commit/unpublish cycle land in between, and the
// head block is then in neither layer: absent from the frozen tx snapshot and
// from an overlay that no longer exists.
func TestGetRawHeader_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := NewPrivateDebugAPI(h.base, db, nil, &rpccfg.DebugApiConfig{})

	header, err := api.GetRawHeader(h.m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(h.overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotEmpty(t, header,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestWithTemporalOverlayPreservesFreezeInfo(t *testing.T) {
	t.Parallel()
	base, m, _ := newOverlayAheadTestAPI(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	want := tx.FreezeInfo()
	view := base.filters.WithTemporalOverlay(tx)
	var got kv.FreezeInfo
	require.NotPanics(t, func() {
		got = view.FreezeInfo()
	})
	require.Equal(t, want, got)
}

func TestRawHeaderAndBlockReturnNullForPublishedPendingBlock(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, nil)
	pendingBlock := types.NewBlockWithHeader(&types.Header{Number: *uint256.NewInt(100)}, nil)
	payload, err := rlp.EncodeToBytes(pendingBlock)
	require.NoError(t, err)
	ff.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{RplBlock: payload})

	base := NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	base._blockReader = failBlockWithSendersReader{
		FullBlockReader: base._blockReader,
		err:             errors.New("pending selector reached block storage"),
	}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)

	tests := map[string]func() (any, error){
		"header": func() (any, error) { return api.GetRawHeader(m.Ctx, pending) },
		"block":  func() (any, error) { return api.GetRawBlock(m.Ctx, pending) },
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := call()
			require.NoError(t, err)
			require.Nil(t, result)
		})
	}
}

func TestHeaderHelpersDoNotReturnPublishedPendingHeader(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, nil)
	pendingBlock := types.NewBlockWithHeader(&types.Header{
		Number:   *uint256.NewInt(100),
		Coinbase: common.Address{1},
	}, nil)
	payload, err := rlp.EncodeToBytes(pendingBlock)
	require.NoError(t, err)
	ff.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{RplBlock: payload})

	base := NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	t.Run("headerByNumber", func(t *testing.T) {
		header, err := base.headerByNumber(m.Ctx, rpc.PendingBlockNumber, tx)
		require.NoError(t, err)
		require.Nil(t, header)
	})

	t.Run("canonicalHeaderByNumberOrHash", func(t *testing.T) {
		header, isLatest, err := base.canonicalHeaderByNumberOrHash(m.Ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber))
		require.NoError(t, err)
		require.False(t, isLatest)
		require.Nil(t, header)
	})
}

func TestHeaderHelpersDoNotReselectOverlay(t *testing.T) {
	tests := []struct {
		name       string
		wantProbes int
		call       func(context.Context, *BaseAPI, kv.Tx) (*types.Header, error)
	}{
		{
			name:       "headerByNumber",
			wantProbes: 1,
			call: func(ctx context.Context, api *BaseAPI, tx kv.Tx) (*types.Header, error) {
				return api.headerByNumber(ctx, rpc.LatestBlockNumber, tx)
			},
		},
		{
			name:       "canonicalHeaderByNumberOrHash",
			wantProbes: 0,
			call: func(ctx context.Context, api *BaseAPI, tx kv.Tx) (*types.Header, error) {
				header, _, err := api.canonicalHeaderByNumberOrHash(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
				return header, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base, m, _, events := newOverlayAheadTestAPIWithEvents(t)
			domains := events.LatestSD()
			require.NotNil(t, domains)
			events.PublishOverlay(nil)

			tx, err := m.DB.BeginTemporalRo(m.Ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			committedHeader, err := m.BlockReader.HeaderByNumber(m.Ctx, tx, overlayRaceChainSize)
			require.NoError(t, err)
			require.NotNil(t, committedHeader)

			probeTx := &publishOverlayOnSecondProbeTx{
				Tx:      tx,
				publish: func() { events.PublishOverlay(domains) },
			}
			header, err := test.call(m.Ctx, base, probeTx)
			require.NoError(t, err)
			require.NotNil(t, header)
			require.Equal(t, committedHeader.Hash(), header.Hash())
			require.Equal(t, test.wantProbes, probeTx.probes)
		})
	}
}

func TestGetBlockNumberPreservesPinnedOverlayView(t *testing.T) {
	base, m, firstHeader, events := newOverlayAheadTestAPIWithEvents(t)

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	pinnedTx := base.filters.WithOverlay(tx)

	replacementTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer replacementTx.Rollback()
	replacementDomains, err := execctx.NewSharedDomains(m.Ctx, replacementTx, m.Log)
	require.NoError(t, err)
	defer replacementDomains.Close()
	require.NoError(t, replacementDomains.InitBlockOverlay(replacementTx, m.Dirs.Tmp))

	replacementHeader := types.CopyHeader(firstHeader)
	replacementHeader.Coinbase = common.Address{2}
	replacementOverlay := replacementDomains.BlockOverlay()
	require.NoError(t, rawdb.WriteHeader(replacementOverlay, replacementHeader))
	require.NoError(t, rawdb.WriteCanonicalHash(replacementOverlay, replacementHeader.Hash(), replacementHeader.Number.Uint64()))
	events.PublishOverlay(replacementDomains)
	defer events.PublishOverlay(nil)

	_, hash, _, err := rpchelper.GetBlockNumber(
		m.Ctx,
		rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(firstHeader.Number.Uint64())),
		pinnedTx,
		m.BlockReader,
		base.filters,
	)
	require.NoError(t, err)
	require.Equal(t, firstHeader.Hash(), hash)
}

func TestReplayTransactionUsesCommittedTxnLookup(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._txnReader = rejectOverlayTxnReader{TxnReader: base._txnReader}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	result, err := api.ReplayTransaction(m.Ctx, common.Hash{1}, []string{TraceTypeTrace}, nil, nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestOtterscanTransactionLookupUsesCommittedView(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._txnReader = rejectOverlayTxnReader{TxnReader: base._txnReader}
	api := NewOtterscanAPI(base, m.DB, 25)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	txn, block, _, _, _, err := api.getTransactionByHash(m.Ctx, tx, common.Hash{1})
	require.NoError(t, err)
	require.Nil(t, txn)
	require.Nil(t, block)
}

func TestCallBundleUsesCommittedTransactionLookup(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	base._txnReader = rejectOverlayTxnReader{TxnReader: base._txnReader}
	api := newEthApiForTest(base, m.DB, nil, nil)

	result, err := api.CallBundle(
		m.Ctx,
		[]common.Hash{{1}},
		rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber),
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestOtterscanTraceUsesUncachedCommittedState(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	base.stateCache = rejectStateCache{}
	api := NewOtterscanAPI(base, m.DB, 25)

	result, err := api.TraceTransaction(m.Ctx, common.HexToHash(debugTraceTransactionTests[0].txHash))
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestOtterscanSearchUsesCommittedView(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base, _, _, _ := newPublishedOverlayTestBase(t, m)

	var committedHash common.Hash
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		var ok bool
		var err error
		committedHash, ok, err = m.BlockReader.CanonicalHash(m.Ctx, tx, overlayRaceChainSize)
		if err != nil {
			return err
		}
		require.True(t, ok)
		return nil
	}))
	reorgHeader := writeOverlayReorgHeader(t, base, m)
	require.NotEqual(t, committedHash, reorgHeader.Hash())

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	results, err := NewOtterscanAPI(base, m.DB, 25).SearchTransactionsAfter(m.Ctx, addr, 3, 10)
	require.NoError(t, err)
	require.NotEmpty(t, results.Txs)
	require.Equal(t, committedHash, *results.Txs[0].BlockHash)
	require.Equal(t, committedHash, results.Receipts[0]["blockHash"])
}

func TestReplayTransactionHandlesMissingHeader(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	minTxNum, err := base._txNumReader.Min(m.Ctx, tx, 1)
	require.NoError(t, err)

	base._txnReader = staticTxnReader{TxnReader: base._txnReader, blockNumber: 1, txNum: minTxNum + 1}
	base._blockReader = hideHeaderBlockReader{FullBlockReader: base._blockReader, blockNumber: 1}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	var result *TraceCallResult
	require.NotPanics(t, func() {
		result, err = api.ReplayTransaction(m.Ctx, common.Hash{1}, []string{TraceTypeTrace}, nil, nil)
	})
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestTraceRawTransactionUsesHeaderCacheInCommittedView(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)

	var latestBlock *types.Block
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		blockNumber, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return err
		}
		latestBlock, err = m.BlockReader.BlockByNumber(m.Ctx, tx, blockNumber)
		return err
	}))
	require.NotNil(t, latestBlock)
	base.blocksLRU.Add(latestBlock.Hash(), latestBlock)
	base._blockReader = failHeaderReadBlockReader{
		FullBlockReader: base._blockReader,
		hash:            latestBlock.Hash(),
		blockNumber:     latestBlock.NumberU64(),
		err:             errors.New("unexpected header database read"),
	}

	encoded, _, _ := rawTxFromBlock(t, m, 6)
	result, err := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{}).RawTransaction(m.Ctx, encoded, []string{TraceTypeTrace})
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestTraceTransactionMethodsUseHeaderCacheInCommittedView(t *testing.T) {
	const blockNumber = uint64(6)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)

	var block *types.Block
	require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error {
		var err error
		block, err = m.BlockReader.BlockByNumber(m.Ctx, tx, blockNumber)
		return err
	}))
	require.NotNil(t, block)
	require.NotEmpty(t, block.Transactions())

	base.blocksLRU.Add(block.Hash(), block)
	base._blockReader = failHeaderReadBlockReader{
		FullBlockReader: base._blockReader,
		hash:            block.Hash(),
		blockNumber:     blockNumber,
		err:             errors.New("unexpected header database read"),
	}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
	txHash := block.Transactions()[0].Hash()

	t.Run("trace_transaction", func(t *testing.T) {
		result, err := api.Transaction(m.Ctx, txHash, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("trace_replayTransaction", func(t *testing.T) {
		result, err := api.ReplayTransaction(m.Ctx, txHash, []string{TraceTypeTrace}, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}

func TestGetTransactionReceiptHandlesMissingHeader(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	minTxNum, err := base._txNumReader.Min(m.Ctx, tx, 1)
	require.NoError(t, err)

	base._txnReader = staticTxnReader{TxnReader: base._txnReader, blockNumber: 1, txNum: minTxNum + 1}
	base._blockReader = hideHeaderBlockReader{FullBlockReader: base._blockReader, blockNumber: 1}
	api := newEthApiForTest(base, m.DB, nil, nil)

	var receipt map[string]any
	require.NotPanics(t, func() {
		receipt, err = api.GetTransactionReceipt(m.Ctx, common.Hash{1})
	})
	require.NoError(t, err)
	require.Nil(t, receipt)
}

// TestDebugAccountAt_OverlayHeadHash_CommittedView pins that debug_accountAt
// resolves the block hash on the committed view: its GetAsOf history reads can
// only see committed data, so an overlay-published head must read as an
// unknown block (null) — not resolve to a header whose canonical-hash check
// then fails.
func TestDebugAccountAt_OverlayHeadHash_CommittedView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	result, err := api.AccountAt(m.Ctx, overlayHeader.Hash(), 0, m.Address)
	require.NoError(t, err, "an in-flight (uncommitted) head hash must read as unknown, not error")
	require.Nil(t, result)
}

func TestGetLogsBlockHashUsesCommittedView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)
	hash := overlayHeader.Hash()

	logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{BlockHash: &hash})
	require.EqualError(t, err, fmt.Sprintf("block not found: %x", hash))
	require.Nil(t, logs)
}

func TestErigonGetLogsByHashPinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, overlayHeader.Number.Uint64()))
	base._blockReader = &unpublishOverlayBlockReader{
		FullBlockReader: base._blockReader,
		events:          events,
		blockNumber:     overlayHeader.Number.Uint64(),
	}
	api := NewErigonAPI(base, m.DB, nil)

	logs, err := api.GetLogsByHash(m.Ctx, overlayHeader.Hash())
	require.NoError(t, err)
	require.NotNil(t, logs)
	require.Empty(t, logs)
}

// TestErigonGetHeaderByNumber_PublishCycleDuringTxAcquisition pins atomic
// acquisition for the erigon header getters, which reach the overlay only
// through headerByNumber / headerByHash and report a missing head as an error.
func TestErigonGetHeaderByNumber_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := NewErigonAPI(h.base, newCycleHookDB(h, true), nil)

	header, err := api.GetHeaderByNumber(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	require.NoError(t, err,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
	require.NotNil(t, header)
}

// TestErigonGetBlockByTimestamp_PublishCycleDuringTxAcquisition pins the
// timestamp search, which bounds itself on the head read through the overlay:
// a cycle landing during the open silently answers from the stale head.
func TestErigonGetBlockByTimestamp_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := NewErigonAPI(h.base, newCycleHookDB(h, true), nil)

	got, err := api.GetBlockByTimestamp(h.m.Ctx, rpc.Timestamp(h.overlayHeader.Time), false)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.Hash(), got["hash"],
		"a publish/commit/unpublish cycle during tx acquisition must not bound the search on the stale head")
}

// TestGetLogs_UsesCommittedFromTag pins that eth_getLogs resolves a "latest"
// fromBlock on the committed view: with the overlay head published ahead of
// MDBX, the tag must not resolve past the executed head and fail the request.
func TestGetLogs_UsesCommittedFromTag(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	_, err := api.GetLogs(h.m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(int64(rpc.LatestBlockNumber))})
	require.NoError(t, err)
}

// TestGetLogs_UsesCommittedToTag is the toBlock counterpart of
// TestGetLogs_UsesCommittedFromTag.
func TestGetLogs_UsesCommittedToTag(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	_, err := api.GetLogs(h.m.Ctx, filters.FilterCriteria{
		FromBlock: big.NewInt(1),
		ToBlock:   big.NewInt(int64(rpc.LatestBlockNumber)),
	})
	require.NoError(t, err)
}

// TestTraceFilter_UsesCommittedFromTag pins that trace_filter resolves a
// "latest" fromBlock on the committed view: with the overlay head published
// ahead of MDBX, the tag must not resolve past a numeric toBlock at the
// executed head.
func TestTraceFilter_UsesCommittedFromTag(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := NewTraceAPI(h.base, h.m.DB, &rpccfg.TraceApiConfig{})

	stream := jsonstream.New(nil)

	from := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	to := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayRaceChainSize))
	err := api.Filter(h.m.Ctx, TraceFilterRequest{FromBlock: &from, ToBlock: &to}, new(bool), nil, stream)
	require.NoError(t, err)
}

// newHeaderAheadTester commits overlayRaceChainSize executed blocks, then
// writes a canonical header one past execution progress directly to the DB.
// This reproduces the window where the headers stage is ahead of execution,
// so a block number resolves while its data is not yet available.
func newHeaderAheadTester(t *testing.T) (m *execmoduletester.ExecModuleTester, aheadHash common.Hash) {
	t.Helper()
	m = execmoduletester.New(t)
	c := insertOverlayRaceChain(t, m)

	aheadNumber := uint64(overlayRaceChainSize) + 1
	header := &types.Header{
		ParentHash: c.TopBlock.Hash(),
		Number:     *uint256.NewInt(aheadNumber),
		Difficulty: *uint256.NewInt(0),
		Time:       c.TopBlock.Time() + 10,
		GasLimit:   30_000_000,
	}
	aheadHash = header.Hash()
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}
		return rawdb.WriteCanonicalHash(tx, aheadHash, aheadNumber)
	}))
	return m, aheadHash
}

// newBlockAheadOfExecutionTester makes the canonical target fully readable as
// a block while leaving its state and transaction-number index unavailable.
func newBlockAheadOfExecutionTester(t *testing.T) (m *execmoduletester.ExecModuleTester, aheadHash common.Hash) {
	t.Helper()
	m, aheadHash = newHeaderAheadTester(t)

	aheadNumber := uint64(overlayRaceChainSize) + 1
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		rawdb.WriteForkchoiceHead(tx, aheadHash)
		return rawdb.WriteBody(tx, aheadHash, aheadNumber, &types.Body{})
	}))
	return m, aheadHash
}

func TestSimulateV1RejectsBlockAheadOfExecution(t *testing.T) {
	m, aheadHash := newBlockAheadOfExecutionTester(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	request := SimulationRequest{BlockStateCalls: []SimulatedBlock{{}}}

	_, err := api.SimulateV1(m.Ctx, request, rpc.BlockNumberOrHashWithHash(aheadHash, false))
	require.ErrorContains(t, err, "not executed")
}

func TestEthWitnessMethodsRejectBlockAheadOfExecution(t *testing.T) {
	m, aheadHash := newBlockAheadOfExecutionTester(t)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	base := newBaseApiForTest(m)
	base._txNumReader = base._txNumReader.WithCustomReadTxNumFunc(rejectTxNumsAboveIndex{
		maxBlock: overlayRaceChainSize,
		err:      errors.New("txnum lookup beyond execution progress"),
	})
	api := newEthApiForTest(base, m.DB, nil, nil)
	selector := rpc.BlockNumberOrHashWithHash(aheadHash, false)

	t.Run("block", func(t *testing.T) {
		_, err := api.GetWitness(m.Ctx, selector)
		require.ErrorContains(t, err, "not executed")
	})

	t.Run("transaction", func(t *testing.T) {
		_, err := api.GetTxWitness(m.Ctx, selector, 0)
		require.ErrorContains(t, err, "not executed")
	})
}

func TestDebugExecutionWitnessRejectsBlockAheadOfExecution(t *testing.T) {
	m, aheadHash := newBlockAheadOfExecutionTester(t)
	base := newBaseApiForTest(m)
	base._txNumReader = base._txNumReader.WithCustomReadTxNumFunc(rejectTxNumsAboveIndex{
		maxBlock: overlayRaceChainSize,
		err:      errors.New("txnum lookup beyond execution progress"),
	})
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	info, err := api.resolveWitnessBlock(m.Ctx, tx, rpc.BlockNumberOrHashWithHash(aheadHash, false))
	require.ErrorContains(t, err, "not executed")
	require.Nil(t, info)
}

func TestStorageRangeAtRejectsBlockAheadOfExecution(t *testing.T) {
	m, aheadHash := newHeaderAheadTester(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})

	_, err := api.StorageRangeAt(m.Ctx, aheadHash, 0, m.Address, nil, 10)
	require.ErrorContains(t, err, "not executed")
}

func TestStorageRangeAtRejectsOverlayOnlyHead(t *testing.T) {
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	_, err := api.StorageRangeAt(m.Ctx, overlayHeader.Hash(), 0, m.Address, nil, 10)
	require.ErrorContains(t, err, "not executed")
}

func TestStorageRangeAtRejectsOverlayReorgAtExecutedHeight(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	reorgHeader := writeOverlayReorgHeader(t, base, m)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	_, err := api.StorageRangeAt(m.Ctx, reorgHeader.Hash(), 0, m.Address, nil, 10)
	require.ErrorContains(t, err, "not available in the committed view")
}

func TestStorageRangeAtPropagatesOverlayProbeError(t *testing.T) {
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	wantErr := errors.New("overlay header lookup failed")
	base._blockReader = failOverlayHeaderNumberBlockReader{FullBlockReader: base._blockReader, err: wantErr}
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	_, err := api.StorageRangeAt(m.Ctx, overlayHeader.Hash(), 0, m.Address, nil, 10)
	require.ErrorIs(t, err, wantErr)
}

func TestStorageRangeAtUnknownHashReturnsEmpty(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	result, err := api.StorageRangeAt(m.Ctx, common.Hash{0xff}, 0, m.Address, nil, 10)
	require.NoError(t, err)
	require.Empty(t, result.Storage)
	require.Nil(t, result.NextKey)
}

func TestAccountRangeRejectsBlockAheadOfExecution(t *testing.T) {
	m, aheadHash := newHeaderAheadTester(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})

	t.Run("hash", func(t *testing.T) {
		selector := rpc.BlockNumberOrHashWithHash(aheadHash, false)
		_, err := api.AccountRange(m.Ctx, selector, m.Address[:], 10, true, true, nil)
		require.ErrorContains(t, err, "not executed")
	})

	t.Run("number", func(t *testing.T) {
		selector := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayRaceChainSize + 1))
		_, err := api.AccountRange(m.Ctx, selector, m.Address[:], 10, true, true, nil)
		require.ErrorContains(t, err, "not executed")
	})
}

func TestAccountRangeLatestUsesExecutionProgress(t *testing.T) {
	m, _ := newBlockAheadOfExecutionTester(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})

	want, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), m.Address[:], 10, true, true, nil)
	require.NoError(t, err)

	got, err := api.AccountRange(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), m.Address[:], 10, true, true, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestAccountAtRejectsBlockAheadOfExecution(t *testing.T) {
	m, aheadHash := newHeaderAheadTester(t)
	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})

	_, err := api.AccountAt(m.Ctx, aheadHash, 0, m.Address)
	require.ErrorContains(t, err, "not executed")
}

func TestGetLogsBlockHashRequiresBody(t *testing.T) {
	t.Parallel()
	m, aheadHash := newHeaderAheadTester(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{BlockHash: &aheadHash})
	require.EqualError(t, err, fmt.Sprintf("block not found: %x", aheadHash))
	require.Nil(t, logs)
}

func TestResolveLogsRangeBlockHashDoesNotDecodeBody(t *testing.T) {
	t.Parallel()
	m := execmoduletester.New(t)
	chainPack := insertOverlayRaceChain(t, m)
	base := newBaseApiForTest(m)
	base._blockReader = failBlockWithSendersReader{
		FullBlockReader: base._blockReader,
		err:             errors.New("unexpected full body read"),
	}
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	hash := chainPack.TopBlock.Hash()

	begin, end, err := base.resolveLogsRange(m.Ctx, tx, filters.FilterCriteria{BlockHash: &hash}, true)
	require.NoError(t, err)
	require.Equal(t, chainPack.TopBlock.NumberU64(), begin)
	require.Equal(t, begin, end)
}

type rejectTxNumsAboveIndex struct {
	maxBlock uint64
	err      error
}

func (r rejectTxNumsAboveIndex) MaxTxNum(ctx context.Context, tx kv.Tx, cursor kv.Cursor, blockNum uint64) (uint64, bool, error) {
	if blockNum > r.maxBlock {
		return 0, false, r.err
	}
	return rawdbv3.DefaultTxBlockIndexInstance.MaxTxNum(ctx, tx, cursor, blockNum)
}

func (r rejectTxNumsAboveIndex) BlockNumber(ctx context.Context, tx kv.Tx, txNum uint64) (uint64, bool, error) {
	return rawdbv3.DefaultTxBlockIndexInstance.BlockNumber(ctx, tx, txNum)
}

// TestTraceFilter_FutureToBlockErrors pins that an explicit toBlock past the
// executed head errors instead of silently clamping the scan to the last
// available txnum, which would make an omitted head block look empty.
func TestTraceFilter_FutureToBlockErrors(t *testing.T) {
	t.Parallel()
	m, _ := newHeaderAheadTester(t)
	api := newTraceApiForTest(m)

	stream := jsonstream.New(nil)

	to := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayRaceChainSize + 1))
	err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &to}, new(bool), nil, stream)
	require.ErrorContains(t, err, "not executed")
}

func TestTraceFilter_FutureFromBlockErrors(t *testing.T) {
	t.Parallel()
	m, _ := newHeaderAheadTester(t)
	api := newTraceApiForTest(m)

	stream := jsonstream.New(nil)

	from := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayRaceChainSize + 1))
	err := api.Filter(m.Ctx, TraceFilterRequest{FromBlock: &from}, new(bool), nil, stream)
	require.ErrorContains(t, err, "not executed")
}

func TestTraceFilter_RejectsOverlayOnlyHead(t *testing.T) {
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	stream := jsonstream.New(nil)
	to := rpc.BlockNumberOrHashWithHash(overlayHeader.Hash(), true)
	err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &to}, new(bool), nil, stream)
	require.ErrorContains(t, err, "not executed")
}

func TestTraceFilter_RejectsOverlayReorgAtExecutedHeight(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	reorgHeader := writeOverlayReorgHeader(t, base, m)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	stream := jsonstream.New(nil)
	to := rpc.BlockNumberOrHashWithHash(reorgHeader.Hash(), true)
	err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &to}, new(bool), nil, stream)
	require.ErrorContains(t, err, "not available in the committed view")
}

func TestTraceFilter_PropagatesOverlayProbeError(t *testing.T) {
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	wantErr := errors.New("overlay header lookup failed")
	base._blockReader = failOverlayHeaderNumberBlockReader{FullBlockReader: base._blockReader, err: wantErr}
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	stream := jsonstream.New(nil)
	to := rpc.BlockNumberOrHashWithHash(overlayHeader.Hash(), true)
	err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &to}, new(bool), nil, stream)
	require.ErrorIs(t, err, wantErr)
}

func TestTraceFilter_UnknownBlockReturnsEmptyArray(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	stream := jsonstream.New(nil)
	to := rpc.BlockNumberOrHashWithHash(common.Hash{0xff}, true)
	err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &to}, new(bool), nil, stream)
	require.NoError(t, err)
	require.Equal(t, "[]", string(stream.Buffer()))
}

func TestTraceFilter_OmittedToBlockUsesExecutionProgress(t *testing.T) {
	m, aheadHash := newHeaderAheadTester(t)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		return rawdb.WriteHeadHeaderHash(tx, aheadHash)
	}))

	base := newBaseApiForTest(m)
	wantErr := errors.New("txnum lookup beyond execution progress")
	base._txNumReader = base._txNumReader.WithCustomReadTxNumFunc(rejectTxNumsAboveIndex{
		maxBlock: overlayRaceChainSize,
		err:      wantErr,
	})
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	stream := jsonstream.New(nil)
	err := api.Filter(m.Ctx, TraceFilterRequest{}, nil, nil, stream)

	require.NoError(t, err)
}

// TestGetModifiedAccountsByHash_FutureStartBlockErrors pins that ByHash rejects
// a not-yet-executed start block like its ByNumber twin, instead of returning
// a silent result from a clamped txnum range.
func TestGetModifiedAccountsByHash_FutureStartBlockErrors(t *testing.T) {
	t.Parallel()
	m, aheadHash := newHeaderAheadTester(t)
	api := newDebugApiForTest(m)

	_, err := api.GetModifiedAccountsByHash(m.Ctx, aheadHash, nil)
	require.ErrorContains(t, err, "later than the latest block")
}

// TestTxPoolContentFrom_UsesOverlayHead pins that txpool_contentFrom reads the
// current header through the block overlay, matching TestTxPoolContent_UsesOverlayHead.
func TestTxPoolContentFrom_UsesOverlayHead(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	pool, txn := newOverlayRacePendingPool(t, h.m)
	api := NewTxPoolAPI(h.base, h.m.DB, pool)

	content, err := api.ContentFrom(h.m.Ctx, h.m.Address)
	require.NoError(t, err)
	got := content["pending"][strconv.FormatUint(txn.GetNonce(), 10)]
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
		"pending tx gas price must be derived from the overlay head's base fee, not the stale MDBX head")
}

// TestFeeHistory_SeesOverlayHead pins that eth_feeHistory resolves "latest" through the
// block overlay: the gas oracle's head must be the in-flight block, so the window ends
// there and oldestBlock is that block. Resolving on the committed view instead leaves the
// whole window one block (or more, while a commit backlog drains) behind the head the node
// publishes via eth_blockNumber.
func TestFeeHistory_SeesOverlayHead(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	got, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.Number.ToBig(), got.OldestBlock.ToInt(),
		"the fee history window must end on the overlay head, not the stale MDBX-committed head")
	require.NotEmpty(t, got.BaseFee)
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.BaseFee[0].ToInt(),
		"the first base fee must come from the overlay head's header")
}

// TestFeeHistory_OverlayHeadWithRewards is TestFeeHistory_SeesOverlayHead with reward
// percentiles requested, which makes the per-block fetch read the in-flight block and its
// receipts instead of just the header — the path that runs on the tx opened by Fork.
// The overlay block carries two txs with distinct tips: a low percentile lands on the
// cheap tx only when the receipts' gas is actually read (zero-filled gas walks the
// percentile cursor to the most expensive tx instead), so the assert pins the receipt
// values, not just the window bounds.
func TestFeeHistory_OverlayHeadWithRewards(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, true)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	got, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, []float64{10})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.Number.ToBig(), got.OldestBlock.ToInt(),
		"the fee history window must end on the overlay head, not the stale MDBX-committed head")
	require.Len(t, got.Reward, 1)
	require.Len(t, got.Reward[0], 1)
	require.Equal(t, big.NewInt(overlayRaceLowTip), got.Reward[0][0].ToInt(),
		"the 10th percentile must be the cheap tx's tip, weighted by the receipts' real gas")
}

// TestGasPriceOracle_ForkKeepsOverlayAfterUnpublish pins that Fork reuses the overlay
// resolved when the backend was built: when the overlay is unpublished between the
// request start and the fork (the commit window closing), the forked backend must
// still serve the head the parent resolved instead of failing with block-not-found.
// The SharedDomains is also closed, as production teardown does right after the
// unpublish, so the test covers reads on a closed-but-pinned overlay.
func TestGasPriceOracle_ForkKeepsOverlayAfterUnpublish(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, h.base.filters.WithTemporalOverlay(tx), h.base)

	h.events.PublishOverlay(nil)
	h.doms.Close()

	require.NoError(t, backend.PrepareFork(h.m.Ctx))
	forked, cleanup, err := backend.Fork(h.m.Ctx)
	require.NoError(t, err)
	require.NotNil(t, forked)
	defer cleanup()

	latest, err := forked.GetLatestBlockNumber()
	require.NoError(t, err)
	require.Equal(t, h.overlayHeader.Number.Uint64(), latest,
		"the forked backend must keep resolving the overlay head the parent pinned")

	got, err := forked.HeaderByNumber(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.Hash(), got.Hash())
}

// publishOverlayHead publishes a fresh overlay whose forkchoice head is the
// given header, with an empty body.
func publishOverlayHead(t *testing.T, h *overlayAheadHarness, head *types.Header) {
	t.Helper()
	require.NoError(t, publishOverlayHeadE(h, head))
}

// publishOverlayHeadE is the hook-safe core: it reports errors instead of
// calling testify's FailNow family, which is test-goroutine-only. t.Cleanup
// is safe from any goroutine.
func publishOverlayHeadE(h *overlayAheadHarness, head *types.Header) error {
	ctx := h.m.Ctx
	roTx, err := h.m.DB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	h.t.Cleanup(roTx.Rollback)
	doms, err := execctx.NewSharedDomains(ctx, roTx, h.m.Log)
	if err != nil {
		return err
	}
	h.t.Cleanup(doms.Close)
	if err := doms.InitBlockOverlay(roTx, h.m.Dirs.Tmp); err != nil {
		return err
	}
	if err := writeHeadBlockMarkersE(doms.BlockOverlay(), head, &types.Body{}); err != nil {
		return err
	}
	h.events.PublishOverlay(doms)
	return nil
}

// publishSiblingOverlay publishes a second overlay holding a same-height
// sibling of the harness's overlay head, with a different base fee so its
// hash and header are distinguishable from the original.
func publishSiblingOverlay(t *testing.T, h *overlayAheadHarness) *types.Header {
	t.Helper()
	sibling := siblingOfOverlayHead(h)
	require.NotEqual(t, h.overlayHeader.Hash(), sibling.Hash())
	publishOverlayHead(t, h, sibling)
	return sibling
}

func siblingOfOverlayHead(h *overlayAheadHarness) *types.Header {
	sibling := types.CopyHeader(h.overlayHeader)
	sibling.BaseFee = uint256.NewInt(overlayRaceBaseFee + 1111)
	return sibling
}

// publishCommittedSiblingOverlay publishes an overlay whose forkchoice head is
// a same-height sibling of an already-committed block — an in-RAM reorg below
// the committed head.
func publishCommittedSiblingOverlay(t *testing.T, h *overlayAheadHarness, number uint64) *types.Header {
	t.Helper()
	roTx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	committed := rawdb.ReadHeaderByNumber(roTx, number)
	require.NotNil(t, committed)

	sibling := types.CopyHeader(committed)
	sibling.BaseFee = uint256.NewInt(overlayRaceBaseFee + 2222)
	require.NotEqual(t, committed.Hash(), sibling.Hash())
	publishOverlayHead(t, h, sibling)
	return sibling
}

// TestGasPriceOracle_PinnedViewIgnoresLaterOverlayPublish pins that a backend
// which resolved overlay A at construction keeps serving A's head even after a
// different overlay B (a same-height sibling, as after an in-RAM reorg) is
// published mid-request: downstream helpers must not layer the live overlay
// over the already-pinned view.
func TestGasPriceOracle_PinnedViewIgnoresLaterOverlayPublish(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, h.base.filters.WithTemporalOverlay(tx), h.base)

	sibling := publishSiblingOverlay(t, h)

	got, err := backend.HeaderByNumber(h.m.Ctx, rpc.LatestBlockNumber)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEqual(t, sibling.Hash(), got.Hash(),
		"the pinned request must not pick up the sibling head published after it started")
	require.Equal(t, h.overlayHeader.Hash(), got.Hash(),
		"the pinned request must keep serving the head it resolved at construction")
}

// TestFeeHistory_DeadOverlayBlockNotServedFromCache pins that fee data computed
// for a not-yet-committed overlay block does not outlive that block: when a
// same-height sibling replaces it (in-RAM reorg, or the commit failing), a new
// request must serve the sibling's fees, not a memoized result keyed only by
// block number.
func TestFeeHistory_DeadOverlayBlockNotServedFromCache(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	first, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), first.BaseFee[0].ToInt())

	sibling := publishSiblingOverlay(t, h)

	second, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, sibling.Number.ToBig(), second.OldestBlock.ToInt())
	require.Equal(t, sibling.BaseFee.ToBig(), second.BaseFee[0].ToInt(),
		"fees cached for the dead overlay block must not be served for its same-height sibling")
}

// TestGasPriceOracle_ForkSharesCallerPinnedOverlay pins that when the caller
// hands the backend a tx already pinned to an overlay (as FillTransaction
// does), Fork wraps its fresh txs with that same overlay: re-capturing the
// live one would let parent and fork resolve two different heads within one
// oracle operation.
func TestGasPriceOracle_ForkSharesCallerPinnedOverlay(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	pinnedTx := h.base.filters.WithTemporalOverlay(tx)

	sibling := publishSiblingOverlay(t, h)

	backend := NewGasPriceOracleBackend(h.m.DB, pinnedTx, h.base)
	parentHead, err := backend.HeaderByNumber(h.m.Ctx, rpc.LatestBlockNumber)
	require.NoError(t, err)
	require.Equal(t, h.overlayHeader.Hash(), parentHead.Hash())

	require.NoError(t, backend.PrepareFork(h.m.Ctx))
	forked, cleanup, err := backend.Fork(h.m.Ctx)
	require.NoError(t, err)
	require.NotNil(t, forked)
	defer cleanup()

	forkHead, err := forked.HeaderByNumber(h.m.Ctx, rpc.LatestBlockNumber)
	require.NoError(t, err)
	require.NotEqual(t, sibling.Hash(), forkHead.Hash(),
		"the fork must not re-capture the overlay published after the caller pinned its tx")
	require.Equal(t, parentHead.Hash(), forkHead.Hash(),
		"parent and fork must resolve the same pinned head")
}

// TestGasPriceOracle_NilOverlayPinIgnoresLaterPublish pins that a backend built
// while no overlay was published keeps reading only committed data: an overlay
// published mid-request (a same-height sibling of the committed head) must not
// leak in through downstream helpers re-resolving the live overlay.
func TestGasPriceOracle_NilOverlayPinIgnoresLaterPublish(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	commitOverlayBlock(t, h)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, rpchelper.PinToOverlay(tx, nil), h.base)

	sibling := publishSiblingOverlay(t, h)

	got, err := backend.HeaderByNumber(h.m.Ctx, rpc.LatestBlockNumber)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEqual(t, sibling.Hash(), got.Hash(),
		"the request pinned to no overlay must not pick up one published mid-request")
	require.Equal(t, h.overlayHeader.Hash(), got.Hash(),
		"the request must keep serving the committed head it resolved at construction")
}

// TestBeginTemporalRoWithOverlay_PreservesOptionalInterfaces pins that the
// pinned handle keeps the tx-scoped block-files view and hands itself — not
// the raw tx — to Apply callbacks, in both the overlay and no-overlay cases:
// dropping either silently degrades every read (per-read view acquisition,
// snapshot-merge straddling) or unpins downstream code.
func TestBeginTemporalRoWithOverlay_PreservesOptionalInterfaces(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)

	check := func(t *testing.T) {
		pinnedTx, err := h.base.filters.BeginTemporalRoWithOverlay(h.m.Ctx, h.m.DB)
		require.NoError(t, err)
		defer pinnedTx.Rollback()

		bf, ok := pinnedTx.(membatchwithdb.HasBlockFilesRoTx)
		require.True(t, ok, "the pinned handle must keep the tx-scoped block-files view")
		require.NotNil(t, bf.BlockFilesRoTx())

		require.NoError(t, pinnedTx.Apply(h.m.Ctx, func(inner kv.Tx) error {
			require.True(t, membatchwithdb.CarriesOverlayView(inner),
				"Apply must hand the pinned view to the callback, not the raw tx")
			return nil
		}))

		require.NotPanics(t, func() { pinnedTx.FreezeInfo() },
			"FreezeInfo must delegate to the raw tx, not promote the view's panic")
		ut, ok := pinnedTx.(interface{ UnderlyingTx() kv.TemporalTx })
		require.True(t, ok, "the pinned handle must not hide UnderlyingTx")
		require.NotNil(t, ut.UnderlyingTx())
		_, ok = pinnedTx.(interface{ Pin() kv.TemporalFilesPin })
		require.True(t, ok, "the pinned handle must not hide Pin")
	}

	t.Run("overlay published", check)
	h.events.PublishOverlay(nil)
	h.doms.Close()
	t.Run("no overlay", check)
}

// TestGasPriceOracleBackend_RequiresPinnedTx pins that the constructor rejects
// an unpinned tx: resolving the overlay after the caller opened the tx would
// re-open the torn (tx, overlay) window the pinned acquisition closes.
func TestGasPriceOracleBackend_RequiresPinnedTx(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.Panics(t, func() { NewGasPriceOracleBackend(h.m.DB, tx, h.base) },
		"an unpinned tx must be rejected, not silently re-pinned to the live overlay")
}

// remoteishBlockReader simulates the rpcdaemon RemoteBlockReader: CanonicalHash
// ignores the caller's tx and resolves on the live view instead.
type remoteishBlockReader struct {
	dbservices.FullBlockReader
	h *overlayAheadHarness
}

func (r *remoteishBlockReader) CanonicalHash(ctx context.Context, _ kv.Getter, blockNum uint64) (common.Hash, bool, error) {
	tx, err := r.h.m.DB.BeginTemporalRo(ctx)
	if err != nil {
		return common.Hash{}, false, err
	}
	defer tx.Rollback()
	return r.FullBlockReader.CanonicalHash(ctx, r.h.base.filters.WithTemporalOverlay(tx), blockNum)
}

// TestGasPriceOracle_CanonicalHashUsesPinnedView pins that the fee-history
// cache key resolves on the pinned view even when the block reader ignores
// the caller's tx (as the rpcdaemon RemoteBlockReader does): a sibling
// published mid-request must not swap the hash under the pinned head.
func TestGasPriceOracle_CanonicalHashUsesPinnedView(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.base._blockReader = &remoteishBlockReader{FullBlockReader: h.m.BlockReader, h: h}

	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, h.base.filters.WithTemporalOverlay(tx), h.base)

	sibling := publishSiblingOverlay(t, h)

	overlayNumber := h.overlayHeader.Number.Uint64()
	hashes, err := backend.CanonicalHashes(h.m.Ctx, overlayNumber, overlayNumber)
	require.NoError(t, err)
	require.Len(t, hashes, 1)
	require.NotEqual(t, sibling.Hash(), hashes[0],
		"the cache key must not come from the live view the reader resolves on")
	require.Equal(t, h.overlayHeader.Hash(), hashes[0],
		"the cache key must resolve on the pinned view")
}

// TestFeeHistory_ReorgedCommittedBlockNotServedFromCache pins that fee data
// memoized for a committed block stops being served once an overlay reorg
// replaces that height: a block number alone does not identify a block.
func TestFeeHistory_ReorgedCommittedBlockNotServedFromCache(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	first, err := api.FeeHistory(h.m.Ctx, 2, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Len(t, first.BaseFee, 3)

	sibling := publishCommittedSiblingOverlay(t, h, overlayRaceChainSize)

	second, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, sibling.Number.ToBig(), second.OldestBlock.ToInt())
	require.Equal(t, sibling.BaseFee.ToBig(), second.BaseFee[0].ToInt(),
		"fees cached for the reorged-out committed block must not be served for its same-height sibling")
}

// commitOverlayBlock writes the harness's overlay head into MDBX the way the
// background commit would, so txs opened afterwards resolve it as the
// committed head.
func commitOverlayBlock(t *testing.T, h *overlayAheadHarness) {
	t.Helper()
	require.NoError(t, commitOverlayBlockE(h))
}

// commitOverlayBlockE writes head markers plus the Execution stage progress
// and the canonical TxNums entry production advances with them: without those
// two, state readers reject the new head.
func commitOverlayBlockE(h *overlayAheadHarness) error {
	rwTx, err := h.m.DB.BeginRw(h.m.Ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()
	if err := writeHeadBlockMarkersE(rwTx, h.overlayHeader, h.overlayBody); err != nil {
		return err
	}
	num := h.overlayHeader.Number.Uint64()
	if txs := h.overlayBody.Transactions; len(txs) > 0 {
		senders := slices.Repeat([]common.Address{h.m.Address}, len(txs))
		if err := rawdb.WriteSenders(rwTx, h.overlayHeader.Hash(), num, senders); err != nil {
			return err
		}
	}
	if err := stages.SaveStageProgress(rwTx, stages.Execution, num); err != nil {
		return err
	}
	if err := rawdb.AppendCanonicalTxNums(rwTx, num); err != nil {
		return err
	}
	return rwTx.Commit()
}

// beginHookDB runs a hook right after each BeginTemporalRo returns, simulating
// commit/publish activity landing while a request acquires its tx. Hooks also
// run on the oracle's errgroup goroutines (via Fork), so they must not use
// testify's FailNow family; failures are reported with the goroutine-safe
// t.Errorf.
type beginHookDB struct {
	kv.TemporalRoDB
	t    *testing.T
	hook func() error
}

func (db *beginHookDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	if hookErr := db.hook(); hookErr != nil {
		db.t.Errorf("begin hook: %v", hookErr)
	}
	return tx, nil
}

// newCycleHookDB returns a DB whose first BeginTemporalRo runs the
// publish/commit/unpublish cycle (optionally skipping the initial publish):
// the window closing while a request acquires its tx.
func newCycleHookDB(h *overlayAheadHarness, publishFirst bool) *beginHookDB {
	return &beginHookDB{TemporalRoDB: h.m.DB, t: h.t, hook: sync.OnceValue(func() error {
		if publishFirst {
			if err := publishOverlayHeadE(h, h.overlayHeader); err != nil {
				return err
			}
		}
		if err := commitOverlayBlockE(h); err != nil {
			return err
		}
		h.events.PublishOverlay(nil)
		return nil
	})}
}

// TestFeeHistory_PublishCycleDuringTxAcquisition pins that overlay-capture
// stability is tracked with a publish sequence number: a full publish/commit/
// unpublish cycle landing during tx acquisition leaves the overlay nil on both
// sides of the open, so pointer identity alone would accept a tx snapshot that
// predates the commit and hides the head block.
func TestFeeHistory_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := newEthApiForTest(h.base, db, nil, nil)

	got, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, h.overlayHeader.Number.ToBig(), got.OldestBlock.ToInt(),
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.BaseFee[0].ToInt())
}

type countCanonicalScansDB struct {
	kv.TemporalRoDB
	scans *atomic.Int64
}

func (db *countCanonicalScansDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	return &countCanonicalScansTx{TemporalTx: tx, scans: db.scans}, nil
}

type countCanonicalScansTx struct {
	kv.TemporalTx
	scans *atomic.Int64
}

// Embedding the tx interface drops BlockFilesRoTx, which block reads require.
func (tx *countCanonicalScansTx) BlockFilesRoTx() *blocksnapshots.View {
	if p, ok := tx.TemporalTx.(membatchwithdb.HasBlockFilesRoTx); ok {
		return p.BlockFilesRoTx()
	}
	return nil
}

func (tx *countCanonicalScansTx) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	if table == kv.HeaderCanonical {
		tx.scans.Add(1)
	}
	return tx.TemporalTx.Range(table, fromPrefix, toPrefix, asc, limit)
}

// TestGasPrice_CachedRequestSkipsCanonicalScan pins that the parent-snapshot
// identity the fan-out needs is resolved only when the request actually fans out.
// Capturing it while the backend is built puts a canonical scan — a remote round
// trip in rpcdaemon mode — on every request, including those the tip cache answers.
func TestGasPrice_CachedRequestSkipsCanonicalScan(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	var scans atomic.Int64
	db := &countCanonicalScansDB{TemporalRoDB: h.m.DB, scans: &scans}
	api := newEthApiForTest(h.base, db, nil, nil)

	_, err := api.GasPrice(h.m.Ctx)
	require.NoError(t, err)
	cold := scans.Load()

	_, err = api.GasPrice(h.m.Ctx)
	require.NoError(t, err)
	require.Equal(t, cold, scans.Load(),
		"a request served from the tip cache must not scan the canonical table")
}

// TestBeginTemporalRoWithOverlay_ChurnPinsNoOverlay pins that a capture which never
// stabilizes ends on an explicit no-overlay pin. Serving the last capture instead
// would layer an overlay generation over a database snapshot the helper just failed
// to match it against: if a sibling was committed in between, the two belong to
// different chains and per-key fallback mixes them.
func TestBeginTemporalRoWithOverlay_ChurnPinsNoOverlay(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	db := &beginHookDB{TemporalRoDB: h.m.DB, t: h.t, hook: func() error {
		return publishOverlayHeadE(h, siblingOfOverlayHead(h))
	}}

	tx, err := h.base.filters.BeginTemporalRoWithOverlay(h.m.Ctx, db)
	require.NoError(t, err)
	defer tx.Rollback()

	overlay, pinned := membatchwithdb.ViewOverlay(tx)
	require.True(t, pinned, "the handle must stay pinned so a later publish cannot enter")
	require.Nil(t, overlay, "an unstable capture must not be pinned as if it were coherent")

	head := rawdb.ReadCurrentHeader(tx)
	require.NotNil(t, head)
	require.Equal(t, uint64(overlayRaceChainSize), head.Number.Uint64(),
		"the request must read the committed head, the only view it can vouch for")
}

// TestFeeHistory_OverlayUnstableDuringTxAcquisition is the end-to-end form of
// the no-overlay pin: a request whose overlay capture never stabilizes still
// answers, on the committed head.
func TestFeeHistory_OverlayUnstableDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	db := &beginHookDB{TemporalRoDB: h.m.DB, t: h.t, hook: func() error { return publishOverlayHeadE(h, siblingOfOverlayHead(h)) }}
	api := newEthApiForTest(h.base, db, nil, nil)

	var committed *types.Header
	require.NoError(t, h.m.DB.View(h.m.Ctx, func(tx kv.Tx) error {
		committed = rawdb.ReadHeaderByNumber(tx, overlayRaceChainSize)
		return nil
	}))
	require.NotNil(t, committed)

	got, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err,
		"a request whose overlay capture never stabilizes must still answer, not fail")
	require.Equal(t, committed.Number.ToBig(), got.OldestBlock.ToInt(),
		"the window must fall back to the committed head, the only coherent view left")
	require.Equal(t, committed.BaseFee.ToBig(), got.BaseFee[0].ToInt())
}

// publishSiblingOnGetCache publishes the sibling overlay the first time the
// gas price oracle consults its cache — after the request has pinned its
// overlay, before the baseFee addend is read.
type publishSiblingOnGetCache struct {
	inner gasprice.Cache
	t     *testing.T
	h     *overlayAheadHarness
	once  sync.Once
}

func (c *publishSiblingOnGetCache) GetLatest() (common.Hash, *uint256.Int) {
	c.once.Do(func() { publishSiblingOverlay(c.t, c.h) })
	return c.inner.GetLatest()
}

func (c *publishSiblingOnGetCache) SetLatest(hash common.Hash, price *uint256.Int) {
	c.inner.SetLatest(hash, price)
}

// TestGasPrice_BaseFeeFromPinnedOverlay pins that eth_gasPrice derives its
// baseFee addend from the same overlay the tip was sampled on: an overlay
// published mid-request (a same-height sibling with a different base fee)
// must not leak into the sum.
func TestGasPrice_BaseFeeFromPinnedOverlay(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	tip, err := api.MaxPriorityFeePerGas(h.m.Ctx)
	require.NoError(t, err)

	api.gasCache = &publishSiblingOnGetCache{inner: api.gasCache, t: t, h: h}

	got, err := api.GasPrice(h.m.Ctx)
	require.NoError(t, err)
	want := new(big.Int).Add(tip.ToInt(), h.overlayHeader.BaseFee.ToBig())
	require.Equal(t, want, got.ToInt(),
		"the baseFee addend must come from the pinned overlay head, not one published mid-request")
}

// TestBlockNumber_PublishCycleDuringTxAcquisition pins that eth_blockNumber
// acquires (tx, overlay) atomically like the fee endpoints: a publish/commit/
// unpublish cycle landing during the open must not hide the head block.
func TestBlockNumber_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := newEthApiForTest(h.base, db, nil, nil)

	got, err := api.BlockNumber(h.m.Ctx)
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(h.overlayHeader.Number.Uint64()), got,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

// TestBaseFee_PublishCycleDuringTxAcquisition pins the same atomic acquisition
// for eth_baseFee: the next-block base fee must derive from the real head.
func TestBaseFee_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := newEthApiForTest(h.base, db, nil, nil)

	got, err := api.BaseFee(h.m.Ctx)
	require.NoError(t, err)
	// the overlay head's GasUsed sits exactly on target, so the next base fee
	// equals its own — the deterministic fingerprint for "derived from head 6"
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.ToInt(),
		"the next-block base fee must derive from the head the cycle committed")
}

// TestFillTransaction_PublishCycleDuringTxAcquisition pins that fee defaults
// price on the head resolved by an atomic acquisition: maxFeePerGas embeds
// 2×baseFee of the head, so a publish/commit/unpublish cycle landing during
// the open must not leave it derived from the previous head.
func TestFillTransaction_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := newEthApiForTest(h.base, db, stubTxPoolClient{}, nil)

	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	nonce := hexutil.Uint64(0)
	result, err := api.FillTransaction(h.m.Ctx, ethapi.CallArgs{From: &h.m.Address, To: &to, Gas: &gas, Nonce: &nonce})
	require.NoError(t, err)
	require.NotNil(t, result.Tx.MaxFeePerGas)
	require.NotNil(t, result.Tx.MaxPriorityFeePerGas)

	baseFeeComponent := new(big.Int).Sub(result.Tx.MaxFeePerGas.ToInt(), result.Tx.MaxPriorityFeePerGas.ToInt())
	require.Equal(t, new(big.Int).Lsh(h.overlayHeader.BaseFee.ToBig(), 1), baseFeeComponent,
		"maxFeePerGas must embed 2×baseFee of the head the cycle committed")
}

// TestFeeHistory_HeadCommittedDuringTxAcquisition pins that the overlay must
// be captured atomically with the tx: when the commit lands and the overlay is
// unpublished between the tx open and the overlay capture, the request would
// otherwise see neither layer and serve a head one block behind the one the
// node already published via eth_blockNumber.
func TestFeeHistory_HeadCommittedDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	db := newCycleHookDB(h, false)
	api := newEthApiForTest(h.base, db, nil, nil)

	got, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, h.overlayHeader.Number.ToBig(), got.OldestBlock.ToInt(),
		"a commit and unpublish landing during tx acquisition must not hide the head block")
	require.Equal(t, h.overlayHeader.BaseFee.ToBig(), got.BaseFee[0].ToInt())
}

// commitSiblingOfCommittedBlock commits a same-height sibling of an already
// committed block, replacing the canonical marker at that height the way a
// reorg landing between two tx opens does.
func commitSiblingOfCommittedBlock(t *testing.T, h *overlayAheadHarness, number uint64) *types.Header {
	t.Helper()
	rwTx, err := h.m.DB.BeginRw(h.m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	committed := rawdb.ReadHeaderByNumber(rwTx, number)
	require.NotNil(t, committed)

	sibling := types.CopyHeader(committed)
	sibling.BaseFee = uint256.NewInt(overlayRaceBaseFee + 3333)
	require.NotEqual(t, committed.Hash(), sibling.Hash())
	writeHeadBlockMarkers(t, rwTx, sibling, &types.Body{})
	require.NoError(t, rwTx.Commit())
	return sibling
}

// TestGasPriceOracle_ForkRejectsDivergentSnapshot pins that a fork is not used
// when its own database snapshot no longer agrees with the chain the parent
// resolved: a child tx never shares the parent's snapshot, so a reorg
// committed after the parent opened would otherwise let one request mix block
// identities from two chains. Fork's documented answer is a nil backend, which
// sends the caller to sequential reads on the parent tx.
func TestGasPriceOracle_ForkRejectsDivergentSnapshot(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, h.base.filters.WithTemporalOverlay(tx), h.base)

	commitSiblingOfCommittedBlock(t, h, overlayRaceChainSize)

	require.NoError(t, backend.PrepareFork(h.m.Ctx))
	forked, cleanup, err := backend.Fork(h.m.Ctx)
	require.NoError(t, err)
	if cleanup != nil {
		cleanup()
	}
	require.Nil(t, forked,
		"a fork whose snapshot carries a reorg the parent never saw must degrade to sequential reads")
}

// TestGasPriceOracle_ForkKeepsParallelAcrossAppend pins the other side of that
// guard: a block appended after the parent opened leaves every identity the
// parent resolved intact, so the fan-out must stay parallel.
func TestGasPriceOracle_ForkKeepsParallelAcrossAppend(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, h.base.filters.WithTemporalOverlay(tx), h.base)

	commitOverlayBlock(t, h)

	require.NoError(t, backend.PrepareFork(h.m.Ctx))
	forked, cleanup, err := backend.Fork(h.m.Ctx)
	require.NoError(t, err)
	require.NotNil(t, forked,
		"an appended block changes no identity the parent resolved, so the fork must stay usable")
	defer cleanup()

	head, err := forked.HeaderByNumber(h.m.Ctx, rpc.LatestBlockNumber)
	require.NoError(t, err)
	require.NotNil(t, head)
	require.Equal(t, h.overlayHeader.Hash(), head.Hash())
}

// unreadableCanonicalTx fails the canonical reads the fork-setup path makes:
// the tip scan on the parent and the marker lookup on the forked tx.
type unreadableCanonicalTx struct {
	kv.TemporalTx
}

func (t unreadableCanonicalTx) Range(table string, from, to []byte, asc order.By, limit int) (stream.KV, error) {
	if table == kv.HeaderCanonical && asc == order.Desc {
		return nil, errors.New("canonical tip unavailable")
	}
	return t.TemporalTx.Range(table, from, to, asc, limit)
}

func (t unreadableCanonicalTx) GetOne(table string, key []byte) ([]byte, error) {
	if table == kv.HeaderCanonical {
		return nil, errors.New("canonical marker unavailable")
	}
	return t.TemporalTx.GetOne(table, key)
}

type unreadableCanonicalDB struct {
	kv.TemporalRoDB
}

func (d unreadableCanonicalDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := d.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic // the caller owns the returned tx
	if err != nil {
		return nil, err
	}
	return unreadableCanonicalTx{TemporalTx: tx}, nil
}

type unopenableDB struct {
	kv.TemporalRoDB
}

func (d unopenableDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	return nil, errors.New("no read transaction available")
}

// TestGasPriceOracle_ForkDegradesOnSetupFailure pins that a failed fork setup
// costs the parallel fan-out, not the request: the answer is still there on the
// parent tx. Each case breaks one of the three steps.
func TestGasPriceOracle_ForkDegradesOnSetupFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prepare require.ErrorAssertionFunc
		backend func(*testing.T, *overlayAheadHarness, kv.TemporalTx) *GasPriceOracleBackend
	}{
		{
			name:    "parent tip unresolved",
			prepare: require.Error,
			backend: func(_ *testing.T, h *overlayAheadHarness, tx kv.TemporalTx) *GasPriceOracleBackend {
				pinned := h.base.filters.WithTemporalOverlay(unreadableCanonicalTx{TemporalTx: tx})
				return NewGasPriceOracleBackend(h.m.DB, pinned, h.base)
			},
		},
		{
			// The identity check runs on the fan-out goroutines, where an error
			// would abort the whole errgroup.
			name:    "identity check unreadable",
			prepare: require.NoError,
			backend: func(t *testing.T, h *overlayAheadHarness, tx kv.TemporalTx) *GasPriceOracleBackend {
				backend := NewGasPriceOracleBackend(unreadableCanonicalDB{h.m.DB}, h.base.filters.WithTemporalOverlay(tx), h.base)
				commitOverlayBlock(t, h) // a snapshot the fork has to compare against the parent
				return backend
			},
		},
		{
			name:    "fork tx cannot open",
			prepare: require.NoError,
			backend: func(_ *testing.T, h *overlayAheadHarness, tx kv.TemporalTx) *GasPriceOracleBackend {
				return NewGasPriceOracleBackend(unopenableDB{h.m.DB}, h.base.filters.WithTemporalOverlay(tx), h.base)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newOverlayAheadHarness(t, false)
			tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			backend := tc.backend(t, h, tx)

			tc.prepare(t, backend.PrepareFork(h.m.Ctx))
			forked, cleanup, err := backend.Fork(h.m.Ctx)
			require.NoError(t, err, "a fork that cannot be set up must not fail the request")
			if cleanup != nil {
				cleanup()
			}
			require.Nil(t, forked)
		})
	}
}

func saveSnapshotsStageProgress(t *testing.T, h *overlayAheadHarness, number uint64) {
	t.Helper()
	rwTx, err := h.m.DB.BeginRw(h.m.Ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	require.NoError(t, stages.SaveStageProgress(rwTx, stages.Snapshots, number))
	require.NoError(t, rwTx.Commit())
}

// TestFeeHistory_SnapshotsStageProgressIsNotTheFrozenBoundary pins that the
// number-keyed cache regime follows what the block reader reports as retired to
// snapshots, not the Snapshots stage progress: that progress tracks
// min(Headers, Bodies, Senders, TxLookup) and on a synced node sits at the
// head, so trusting it keys reorgable heights by number and lets a dead block
// be served from the cache.
func TestFeeHistory_SnapshotsStageProgressIsNotTheFrozenBoundary(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	saveSnapshotsStageProgress(t, h, overlayRaceChainSize)
	api := newEthApiForTest(h.base, h.m.DB, nil, nil)

	first, err := api.FeeHistory(h.m.Ctx, 2, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Len(t, first.BaseFee, 3)

	sibling := publishCommittedSiblingOverlay(t, h, overlayRaceChainSize)

	second, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, sibling.Number.ToBig(), second.OldestBlock.ToInt())
	require.Equal(t, sibling.BaseFee.ToBig(), second.BaseFee[0].ToInt(),
		"a height the Snapshots stage progress covers is still reorgable, so it must not be cached by number")
}

type frozenBlocksReader struct {
	dbservices.FullBlockReader
	frozen uint64
}

func (r frozenBlocksReader) FrozenBlocks() uint64 { return r.frozen }

// TestGasPriceOracleBackend_FrozenBoundaryComesFromSnapshots pins the other side
// of that boundary: what the block reader reports as retired to snapshots is the
// only height range the canonical mapping can no longer change on.
func TestGasPriceOracleBackend_FrozenBoundaryComesFromSnapshots(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.base._blockReader = frozenBlocksReader{FullBlockReader: h.base._blockReader, frozen: 7}

	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, h.base.filters.WithTemporalOverlay(tx), h.base)

	require.Equal(t, uint64(7), backend.FrozenBlocks())
}

// newOverlayReceiptsUnpublishTestAPI publishes an overlay whose head block carries a
// transaction and executed state, then drops the overlay while its canonical hash is
// read. A handler that selects a view once survives it; one that selects again mid-way
// resolves against a generation that is no longer published.
func newOverlayReceiptsUnpublishTestAPI(t *testing.T) (*BaseAPI, *execmoduletester.ExecModuleTester, *types.Header) {
	t.Helper()
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	// Nonce zero: the receipts of this block are derived by replay, so the transaction
	// has to be executable against the state the overlay publishes.
	txn := signOverlayRaceTestTx(t, m, 0)
	require.NoError(t, rawdb.WriteBody(overlay, overlayHeader.Hash(), overlayHeader.Number.Uint64(), &types.Body{Transactions: []types.Transaction{txn}}))
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, overlayHeader.Number.Uint64()))
	base._blockReader = &unpublishOverlayBlockReader{
		FullBlockReader: base._blockReader,
		events:          events,
		blockNumber:     overlayHeader.Number.Uint64(),
	}
	return base, m, overlayHeader
}

func TestOtsGetBlockDetails_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayReceiptsUnpublishTestAPI(t)
	api := NewOtterscanAPI(base, m.DB, 25)

	details, err := api.GetBlockDetails(m.Ctx, rpc.BlockNumber(overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, details)
}

// TestOtsGetBlockDetails_PublishCycleDuringTxAcquisition and its GraphQL twin
// pin atomic acquisition for the two block-detail families.
func TestOtsGetBlockDetails_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	api := NewOtterscanAPI(h.base, newCycleHookDB(h, true), 25)

	details, err := api.GetBlockDetails(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, details,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestGraphQLGetBlockDetails_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := NewGraphQLAPI(h.base, db, newEthApiForTest(h.base, db, nil, nil), nil, &rpccfg.GraphQLApiConfig{})

	details, err := api.GetBlockDetails(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, details,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

func TestOtsGetBlockTransactions_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayReceiptsUnpublishTestAPI(t)
	api := NewOtterscanAPI(base, m.DB, 25)

	result, err := api.GetBlockTransactions(m.Ctx, rpc.BlockNumber(overlayHeader.Number.Uint64()), 0, 10)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestGraphQLGetBlockDetails_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayReceiptsUnpublishTestAPI(t)
	api := NewGraphQLAPI(base, m.DB, newEthApiForTest(base, m.DB, nil, nil), nil, &rpccfg.GraphQLApiConfig{})

	details, err := api.GetBlockDetails(m.Ctx, rpc.BlockNumber(overlayHeader.Number.Uint64()))
	require.NoError(t, err)
	require.NotNil(t, details)
}

// TestCall_PublishCycleDuringTxAcquisition pins that eth_call acquires
// (tx, overlay) atomically: a publish/commit/unpublish cycle landing between
// the tx open and the overlay capture leaves the head block visible in neither
// layer, so the call would resolve against a snapshot that predates the commit.
func TestCall_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := newEthApiForTest(h.base, db, nil, nil)

	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	blockNr := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(h.overlayHeader.Number.Uint64()))
	_, err := api.Call(h.m.Ctx, ethapi.CallArgs{From: &h.m.Address, To: &to}, &blockNr, nil, nil)
	require.NoError(t, err,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}

// TestGraphQLCall_PublishCycleDuringTxAcquisition is the GraphQL twin of
// TestCall_PublishCycleDuringTxAcquisition: the same handler shape, so the
// same torn (tx, overlay) acquisition hides the head block.
func TestGraphQLCall_PublishCycleDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	h.events.PublishOverlay(nil)
	h.doms.Close()

	db := newCycleHookDB(h, true)
	api := NewGraphQLAPI(h.base, db, newEthApiForTest(h.base, db, nil, nil), nil,
		&rpccfg.GraphQLApiConfig{GasCap: 50_000_000})

	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	_, err := api.Call(h.m.Ctx, rpc.BlockNumber(h.overlayHeader.Number.Uint64()),
		ethapi.CallArgs{From: &h.m.Address, To: &to})
	require.NoError(t, err,
		"a publish/commit/unpublish cycle during tx acquisition must not hide the head block")
}
