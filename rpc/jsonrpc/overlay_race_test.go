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
	"math/big"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
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
// With withOverlayTxs the block instead carries two transactions with distinct
// tips plus their receipt-domain entries, and GasUsed equals the transactions'
// real total so reward percentile thresholds relate to the receipts' gas.
func newOverlayAheadHarness(t *testing.T, withOverlayTxs bool) *overlayAheadHarness {
	t.Helper()

	var cfg chain.Config
	require.NoError(t, copier.CopyWithOption(&cfg, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
	cfg.LondonBlock = common.NewUint64(0)
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(&cfg))

	c := insertOverlayRaceChain(t, m)

	ctx := m.Ctx
	overlayRoTx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	t.Cleanup(overlayRoTx.Rollback)
	doms, err := execctx.NewSharedDomains(ctx, overlayRoTx, m.Log)
	require.NoError(t, err)
	t.Cleanup(doms.Close)
	require.NoError(t, doms.InitBlockOverlay(overlayRoTx, m.Dirs.Tmp))

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
	overlay := doms.BlockOverlay()
	writeHeadBlockMarkers(t, overlay, overlayHeader, &types.Body{Transactions: overlayTxs})

	if withOverlayTxs {
		senders := slices.Repeat([]common.Address{m.Address}, len(overlayTxs))
		require.NoError(t, rawdb.WriteSenders(overlay, hash, overlayNumber, senders))
		// Receipt-domain entries go through the SharedDomains (like execution writes
		// them), so readers reach them via the view's DomainReader, not the overlay tables.
		minTxNum, err := m.BlockReader.TxnumReader().Min(ctx, overlayRoTx, overlayNumber)
		require.NoError(t, err)
		putDel := doms.AsPutDel(overlayRoTx)
		var cumGas uint64
		for i := range overlayTxs {
			cumGas += params.TxGas
			require.NoError(t, rawtemporaldb.AppendReceiptMetadata(putDel, 0, cumGas, 0, minTxNum+1+uint64(i)))
		}
	}

	events := shards.NewEvents()
	events.PublishOverlay(doms)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := newBaseApiWithFiltersForTest(ff, stateCache, m)

	return &overlayAheadHarness{t: t, base: base, m: m, overlayHeader: overlayHeader, events: events, doms: doms}
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
	require.Equal(t, h.overlayHeader.Number.ToBig(), resp["number"].(*hexutil.Big).ToInt(),
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

	s := jsoniter.ConfigDefault.BorrowStream(nil)
	defer jsoniter.ConfigDefault.ReturnStream(s)
	stream := jsonstream.Wrap(s)

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

// TestTraceFilter_FutureToBlockErrors pins that an explicit toBlock past the
// executed head errors instead of silently clamping the scan to the last
// available txnum, which would make an omitted head block look empty.
func TestTraceFilter_FutureToBlockErrors(t *testing.T) {
	t.Parallel()
	m, _ := newHeaderAheadTester(t)
	api := newTraceApiForTest(m)

	s := jsoniter.ConfigDefault.BorrowStream(nil)
	defer jsoniter.ConfigDefault.ReturnStream(s)
	stream := jsonstream.Wrap(s)

	to := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayRaceChainSize + 1))
	err := api.Filter(m.Ctx, TraceFilterRequest{ToBlock: &to}, new(bool), nil, stream)
	require.ErrorContains(t, err, "not executed")
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
	if err := writeHeadBlockMarkersE(rwTx, h.overlayHeader, &types.Body{}); err != nil {
		return err
	}
	num := h.overlayHeader.Number.Uint64()
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

// TestFeeHistory_OverlayUnstableDuringTxAcquisition pins that when the overlay
// keeps changing across every acquisition attempt (a fresh sibling published
// on every open), the request still serves the last capture as one pinned
// view instead of failing: under FCU churn a slightly stale answer beats a
// client-visible error.
func TestFeeHistory_OverlayUnstableDuringTxAcquisition(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	db := &beginHookDB{TemporalRoDB: h.m.DB, t: h.t, hook: func() error { return publishOverlayHeadE(h, siblingOfOverlayHead(h)) }}
	api := newEthApiForTest(h.base, db, nil, nil)

	got, err := api.FeeHistory(h.m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err,
		"a request whose overlay capture never stabilizes must serve the last capture, not fail")
	require.Equal(t, h.overlayHeader.Number.ToBig(), got.OldestBlock.ToInt())
	require.Equal(t, big.NewInt(overlayRaceBaseFee+1111), got.BaseFee[0].ToInt(),
		"the window must come from one pinned sibling view")
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
