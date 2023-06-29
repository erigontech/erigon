package commands

import (
	"context"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
)

func TestGasPrice(t *testing.T) {

	cases := []struct {
		description   string
		chainSize     int
		expectedPrice *big.Int
	}{
		{
			description:   "standard settings 60 blocks",
			chainSize:     60,
			expectedPrice: big.NewInt(params.GWei * int64(36)),
		},
		{
			description:   "standard settings 30 blocks",
			chainSize:     30,
			expectedPrice: big.NewInt(params.GWei * int64(18)),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.description, func(t *testing.T) {
			m := createGasPriceTestKV(t, testCase.chainSize)
			defer m.DB.Close()
			eth := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())

			ctx := context.Background()
			result, err := eth.GasPrice(ctx)
			if err != nil {
				t.Fatalf("error getting gas price: %s", err)
			}

			if testCase.expectedPrice.Cmp(result.ToInt()) != 0 {
				t.Fatalf("gas price mismatch, want %d, got %d", testCase.expectedPrice, result.ToInt())
			}
		})
	}

}

func createGasPriceTestKV(t *testing.T, chainSize int) *stages.MockSentry {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := stages.MockWithGenesis(t, gspec, key, false)

	// Generate testing blocks
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), libcommon.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*params.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	})
	if err != nil {
		t.Error(err)
	}
	// Construct testing chain
	if err = m.InsertChain(chain, nil); err != nil {
		t.Error(err)
	}

	return m
}
