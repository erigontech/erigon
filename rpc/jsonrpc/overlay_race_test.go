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
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
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
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const (
	overlayRaceChainSize = 5
	overlayRaceBaseFee   = 424242
	overlayRaceLowTip    = 1_000_000
	overlayRaceHighTip   = 2_000_000
)

type overlayAheadHarness struct {
	base          *BaseAPI
	m             *execmoduletester.ExecModuleTester
	overlayHeader *types.Header
	events        *shards.Events
}

func newOverlayAheadTestAPI(t *testing.T) (base *BaseAPI, m *execmoduletester.ExecModuleTester, overlayHeader *types.Header) {
	t.Helper()
	h := newOverlayAheadHarness(t, false)
	return h.base, h.m, h.overlayHeader
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

	c, err := m.GenerateChain(overlayRaceChainSize, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

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
	// Minimal subset of what InsertBlocks/updateForkChoice write in production,
	// enough for the reader paths under test to resolve this header as current.
	require.NoError(t, rawdb.WriteHeader(overlay, overlayHeader))
	require.NoError(t, rawdb.WriteHeadHeaderHash(overlay, hash))
	rawdb.WriteForkchoiceHead(overlay, hash)
	require.NoError(t, rawdb.WriteCanonicalHash(overlay, hash, overlayNumber))
	require.NoError(t, rawdb.WriteBody(overlay, hash, overlayNumber, &types.Body{Transactions: overlayTxs}))
	// The forkchoice marker is what rpchelper.GetLatestBlockNumber resolves the head from,
	// so readers that go through it (the gas oracle, "latest" tag resolution) see this block.
	rawdb.WriteForkchoiceHead(overlay, hash)

	if withOverlayTxs {
		senders := make([]common.Address, len(overlayTxs))
		for i := range senders {
			senders[i] = m.Address
		}
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
	filters := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := newBaseApiWithFiltersForTest(filters, stateCache, m)

	return &overlayAheadHarness{base: base, m: m, overlayHeader: overlayHeader, events: events}
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

// TestFeeHistory_SeesOverlayHead pins that eth_feeHistory resolves "latest" through the
// block overlay: the gas oracle's head must be the in-flight block, so the window ends
// there and oldestBlock is that block. Resolving on the committed view instead leaves the
// whole window one block (or more, while a commit backlog drains) behind the head the node
// publishes via eth_blockNumber.
func TestFeeHistory_SeesOverlayHead(t *testing.T) {
	t.Parallel()
	base, m, overlayHeader := newOverlayAheadTestAPI(t)
	api := newEthApiForTest(base, m.DB, nil, nil)

	got, err := api.FeeHistory(m.Ctx, 1, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, overlayHeader.Number.ToBig(), got.OldestBlock.ToInt(),
		"the fee history window must end on the overlay head, not the stale MDBX-committed head")
	require.NotEmpty(t, got.BaseFee)
	require.Equal(t, overlayHeader.BaseFee.ToBig(), got.BaseFee[0].ToInt(),
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
func TestGasPriceOracle_ForkKeepsOverlayAfterUnpublish(t *testing.T) {
	t.Parallel()
	h := newOverlayAheadHarness(t, false)
	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	backend := NewGasPriceOracleBackend(h.m.DB, tx, h.base)

	h.events.PublishOverlay(nil)

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
