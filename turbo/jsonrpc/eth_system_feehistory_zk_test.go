package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi/bind/backends"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/holiman/uint256"
)

type mockTrackerFH struct{ tip *big.Int }

func (m *mockTrackerFH) GetLatestPrice() (*big.Int, error) { return new(big.Int).Set(m.tip), nil }
func (m *mockTrackerFH) GetLowestPrice() *big.Int          { return new(big.Int).Set(m.tip) }

func newZkSimBackend(t *testing.T) (*backends.SimulatedBackend, *types.Genesis, libcommon.Address, libcommon.Address, *types.GenesisAlloc) {
	t.Helper()
	// Keys/addresses
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	key2, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	addr := crypto.PubkeyToAddress(key.PublicKey)
	addr2 := crypto.PubkeyToAddress(key2.PublicKey)

	// Clone the test chain config and set a ZK chain id
    cfg := *params.TestChainConfig
    cfg.ChainID = big.NewInt(999999)           // Local ZK devnet id
    cfg.LondonBlock = big.NewInt(0)            // Enable EIP-1559 from genesis

	alloc := types.GenesisAlloc{
		addr:  {Balance: big.NewInt(1e18)},
		addr2: {Balance: big.NewInt(1e18)},
	}
	gspec := &types.Genesis{Config: &cfg, Alloc: alloc, GasLimit: 15_000_000}

	be := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	be.Commit() // mine an initial empty block
	return be, gspec, addr, addr2, &alloc
}

func newEthAPIForBackend(t *testing.T, be *backends.SimulatedBackend, tracker RpcL1GasPriceTracker) *APIImpl {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	base := NewBaseApi(nil, stateCache, be.BlockReader(), be.Agg(), false, rpccfg.DefaultEvmCallTimeout, be.Engine(), datadir.New(t.TempDir()))
	return NewEthAPI(base, be.DB(), nil, nil, nil, 5_000_000, 100_000, 100_000, &ethconfig.Defaults, false, 100, 100, log.New(), tracker, 1000, false)
}

func TestFeeHistory_ZkSequencer_EmptyBlocks(t *testing.T) {
	t.Setenv("CDK_ERIGON_SEQUENCER", "1")
	be, _, _, _, _ := newZkSimBackend(t)
	defer be.Close()

	// Mine a few empty blocks
	be.Commit()
	be.Commit()
	be.Commit()

	// Tracker floor used to fill empty rows (higher percentiles)
	floor := big.NewInt(1_234_567)
	eth := newEthAPIForBackend(t, be, &mockTrackerFH{tip: floor})

	ctx := context.Background()
	res, err := eth.FeeHistory(ctx, rpc.DecimalOrHex(3), rpc.LatestBlockNumber, []float64{10, 50, 90})
	if err != nil {
		t.Fatalf("FeeHistory error: %v", err)
	}
	if len(res.Reward) == 0 {
		t.Fatalf("expected non-empty reward rows")
	}
	// Each row should be [0, floor, floor] for empty blocks
	for i := range res.Reward {
		row := res.Reward[i]
		if len(row) != 3 {
			t.Fatalf("unexpected reward row len: %d", len(row))
		}
		if row[0].ToInt().Sign() != 0 {
			t.Fatalf("row[%d][0] expected 0, got %s", i, row[0].ToInt())
		}
		if row[1].ToInt().Cmp(floor) != 0 || row[2].ToInt().Cmp(floor) != 0 {
			t.Fatalf("row[%d] expected floor=%s at 50/90, got %s/%s", i, floor, row[1].ToInt(), row[2].ToInt())
		}
	}
}

func TestFeeHistory_ZkSequencer_WithTxs(t *testing.T) {
	t.Setenv("CDK_ERIGON_SEQUENCER", "1")
	be, gspec, from, to, _ := newZkSimBackend(t)
	defer be.Close()

	// Create a single EIP-1559 tx with a known tip; fee cap is large so effective tip = tip
	nonce, err := be.PendingNonceAt(context.Background(), from)
	if err != nil {
		t.Fatalf("nonce err: %v", err)
	}
	tip := uint256.NewInt(1_000_000)        // 1e6 wei
	feecap := uint256.NewInt(1_000_000_000) // 1e9 wei
	tx := types.NewEIP1559Transaction(*uint256.NewInt(gspec.Config.ChainID.Uint64()), nonce, to, uint256.NewInt(0), 21_000, uint256.NewInt(1), tip, feecap, nil)
	signer := types.LatestSigner(gspec.Config)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	signed, err := types.SignNewTx(key, *signer, tx)
	if err != nil {
		t.Fatalf("sign err: %v", err)
	}
	if err := be.SendTransaction(context.Background(), signed); err != nil {
		t.Fatalf("send err: %v", err)
	}
	be.Commit() // mine block with the tx

	// Tracker shouldn't affect non-empty blocks; still provide one
	eth := newEthAPIForBackend(t, be, &mockTrackerFH{tip: big.NewInt(7777777)})

	ctx := context.Background()
	res, err := eth.FeeHistory(ctx, rpc.DecimalOrHex(1), rpc.LatestBlockNumber, []float64{10, 50, 90})
	if err != nil {
		t.Fatalf("FeeHistory error: %v", err)
	}
	if len(res.Reward) != 1 {
		t.Fatalf("expected 1 reward row, got %d", len(res.Reward))
	}
	row := res.Reward[0]
	if len(row) != 3 {
		t.Fatalf("unexpected reward row len: %d", len(row))
	}
	// With a single tx in block, all percentiles should equal the tx tip
	want := big.NewInt(1_000_000)
	for j, v := range row {
		if v == nil {
			t.Fatalf("row[%d] percentile %d is nil", 0, j)
		}
		if v.ToInt().Cmp(want) != 0 {
			t.Fatalf("row[0][%d] want %s, got %s", j, want.String(), v.ToInt().String())
		}
	}
}
