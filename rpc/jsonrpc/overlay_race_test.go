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
	"strconv"
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
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const (
	overlayRaceChainSize = 5
	overlayRaceBaseFee   = 424242
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

// newOverlayAheadTestAPI builds overlayRaceChainSize committed MDBX blocks,
// then publishes a fabricated block one past them (overlayRaceChainSize+1)
// into the block overlay only, never committed to MDBX. This reproduces the
// window where forkchoice publishes the overlay before the MDBX commit
// lands, so a plain tx would still report the previous head.
//
// The overlay block's GasUsed is set to exactly its EIP-1559 target so
// misc.CalcBaseFee leaves BaseFee unchanged, making overlayRaceBaseFee a
// reliable, deterministic fingerprint for "the code read the overlay head".
func newOverlayAheadTestAPI(t *testing.T) (base *BaseAPI, m *execmoduletester.ExecModuleTester, overlayHeader *types.Header) {
	base, m, overlayHeader, _ = newOverlayAheadTestAPIWithEvents(t)
	return base, m, overlayHeader
}

func newOverlayAheadTestAPIWithEvents(t *testing.T) (base *BaseAPI, m *execmoduletester.ExecModuleTester, overlayHeader *types.Header, events *shards.Events) {
	t.Helper()

	var cfg chain.Config
	require.NoError(t, copier.CopyWithOption(&cfg, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
	cfg.LondonBlock = common.NewUint64(0)
	m = execmoduletester.New(t, execmoduletester.WithChainConfig(&cfg))

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
	overlayHeader = &types.Header{
		ParentHash: c.TopBlock.Hash(),
		Number:     *uint256.NewInt(overlayNumber),
		Difficulty: *uint256.NewInt(0),
		Time:       c.TopBlock.Time() + 10,
		GasLimit:   overlayGasLimit,
		GasUsed:    overlayGasLimit / params.ElasticityMultiplier, // == target: CalcBaseFee leaves BaseFee unchanged
		BaseFee:    uint256.NewInt(overlayRaceBaseFee),
	}
	hash := overlayHeader.Hash()
	overlay := doms.BlockOverlay()
	// Minimal subset of what InsertBlocks/updateForkChoice write in production,
	// enough for the reader paths under test to resolve this header as current.
	require.NoError(t, rawdb.WriteHeader(overlay, overlayHeader))
	require.NoError(t, rawdb.WriteHeadHeaderHash(overlay, hash))
	rawdb.WriteForkchoiceHead(overlay, hash)
	require.NoError(t, rawdb.WriteCanonicalHash(overlay, hash, overlayNumber))
	require.NoError(t, rawdb.WriteBody(overlay, hash, overlayNumber, &types.Body{}))

	events = shards.NewEvents()
	events.PublishOverlay(doms)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base = newBaseApiWithFiltersForTest(ff, stateCache, m)

	return base, m, overlayHeader, events
}

type unpublishOverlayBlockReader struct {
	dbservices.FullBlockReader
	events      *shards.Events
	blockNumber uint64
}

type rejectOverlayTxnReader struct {
	dbservices.TxnReader
}

func (r rejectOverlayTxnReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, uint64, bool, error) {
	if view, ok := tx.(interface{ IsOverlayReadView() bool }); ok && view.IsOverlayReadView() {
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
	signer := types.LatestSigner(m.ChainConfig)
	txn, err := types.SignTx(
		types.NewEIP1559Transaction(*m.ChainConfig.ChainID, nonce, common.HexToAddress("deadbeef"), uint256.NewInt(1), 21000, nil, uint256.NewInt(0), uint256.NewInt(1_000_000_000_000), nil),
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
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := NewErigonAPI(base, m.DB, nil)

	resp, err := api.GetBlockByTimestamp(m.Ctx, rpc.Timestamp(overlayHeader.Time), false)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, overlayHeader.Number.ToBig(), resp["number"].(*hexutil.Big).ToInt(),
		"must resolve to the overlay head block, not the stale MDBX-committed head")
}

func TestGetModifiedAccountsByNumber_UsesCommittedStartTag(t *testing.T) {
	t.Parallel()
	base, m, _ := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	result, err := api.GetModifiedAccountsByNumber(m.Ctx, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}

func TestGetModifiedAccountsByNumber_UsesCommittedEndTag(t *testing.T) {
	t.Parallel()
	base, m, _ := newOverlayAheadTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
	latest := rpc.LatestBlockNumber

	result, err := api.GetModifiedAccountsByNumber(m.Ctx, rpc.EarliestBlockNumber, &latest)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}

// TestGetTransactionByHash_PendingTx_UsesOverlayHead pins that the pending-tx
// fallback in GetTransactionByHash reads the current header through the block
// overlay: the returned tx's gas price (derived from that header's base fee)
// must reflect the overlay head, not the stale MDBX-committed head.
func TestGetTransactionByHash_PendingTx_UsesOverlayHead(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)

	pendingTxn := signOverlayRaceTestTx(t, m, 1)
	pool := &overlayRaceTxPoolClient{
		transactionsReply: &txpoolproto.TransactionsReply{RlpTxs: [][]byte{marshalOverlayRaceTestTx(t, pendingTxn)}},
	}
	api := newEthApiForTest(base, m.DB, pool, nil)

	got, err := api.GetTransactionByHash(m.Ctx, pendingTxn.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
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
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	pool, txn := newOverlayRacePendingPool(t, m)
	api := NewTxPoolAPI(base, m.DB, pool)

	content, err := api.Content(m.Ctx)
	require.NoError(t, err)
	got := content["pending"][m.Address.Hex()][strconv.FormatUint(txn.GetNonce(), 10)]
	require.NotNil(t, got)
	require.Equal(t, overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
		"pending tx gas price must be derived from the overlay head's base fee, not the stale MDBX head")
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

func TestGetRawHeader_PinsOverlayView(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
	api := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})

	header, err := api.GetRawHeader(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayHeader.Number.Uint64())))
	require.NoError(t, err)
	require.NotNil(t, header)
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

// TestGetLogs_UsesCommittedFromTag pins that eth_getLogs resolves a "latest"
// fromBlock on the committed view: with the overlay head published ahead of
// MDBX, the tag must not resolve past the executed head and fail the request.
func TestGetLogs_UsesCommittedFromTag(t *testing.T) {
	t.Parallel()
	base, m, _ := newOverlayAheadTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	_, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(int64(rpc.LatestBlockNumber))})
	require.NoError(t, err)
}

// TestGetLogs_UsesCommittedToTag is the toBlock counterpart of
// TestGetLogs_UsesCommittedFromTag.
func TestGetLogs_UsesCommittedToTag(t *testing.T) {
	t.Parallel()
	base, m, _ := newOverlayAheadTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	_, err := api.GetLogs(m.Ctx, filters.FilterCriteria{
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
	base, m, _ := newOverlayAheadTestAPI(t)
	api := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	s := jsoniter.ConfigDefault.BorrowStream(nil)
	defer jsoniter.ConfigDefault.ReturnStream(s)
	stream := jsonstream.Wrap(s)

	from := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	to := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(overlayRaceChainSize))
	err := api.Filter(m.Ctx, TraceFilterRequest{FromBlock: &from, ToBlock: &to}, new(bool), nil, stream)
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
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	pool, txn := newOverlayRacePendingPool(t, m)
	api := NewTxPoolAPI(base, m.DB, pool)

	content, err := api.ContentFrom(m.Ctx, m.Address)
	require.NoError(t, err)
	got := content["pending"][strconv.FormatUint(txn.GetNonce(), 10)]
	require.NotNil(t, got)
	require.Equal(t, overlayHeader.BaseFee.ToBig(), got.GasPrice.ToInt(),
		"pending tx gas price must be derived from the overlay head's base fee, not the stale MDBX head")
}
