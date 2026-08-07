package jsonrpc

// A block that deploys a contract writes code-chunk leaves. Under bin the
// witness pass walks the parent state, where that code does not exist yet, so
// the chunk keys have to come from the block's own code — otherwise the nodes
// those insertions split go unproved and a stateless verifier cannot reach the
// post-state root.
//
// The second deploy is what makes this bite: the first lands in an empty code
// zone and splits nothing.

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestPBinWitnessConsecutiveSpillingDeploys(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bankAddress := crypto.PubkeyToAddress(bankKey.PublicKey)
	bankFunds, ok := new(big.Int).SetString("100000000000000000000", 10)
	require.True(t, ok)

	chainConfig := new(chain.Config)
	require.NoError(t, copier.CopyWithOption(chainConfig, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config:   chainConfig,
			Alloc:    types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
			GasLimit: 60_000_000,
		}),
		execmoduletester.WithKey(bankKey))

	signer := types.LatestSignerForChainID(nil)
	const deploys = 3
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, deploys, func(i int, b *blockgen.BlockGen) {
		// Each contract spills past the account header by a few chunks, and each
		// is distinct, so every deploy opens its own code-zone stem beside the
		// ones already there.
		runtime := make([]byte, pbinHeaderCodeCapacity+31*(i+2))
		for j := range runtime {
			runtime[j] = 0xfe
		}
		copy(runtime, pbinStoreRuntime)
		runtime[len(runtime)-1] = byte(i)

		nonce := b.TxNonce(bankAddress)
		txn := &types.LegacyTx{CommonTx: types.CommonTx{
			Nonce: nonce, GasLimit: 12_000_000, Data: pbinDeployCode(runtime),
		}}
		txn.GasPrice = *uint256.NewInt(1_000_000_000)
		signed, err := types.SignTx(txn, *signer, bankKey)
		require.NoError(t, err)
		b.AddTx(signed)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))

	c := &pbinWitnessChain{m: m, pack: pack}
	enableCommitmentHistoryFlag(t, c.m.DB)
	api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})

	for block := uint64(1); block <= deploys; block++ {
		// pbinWitnessOf fails the test if stateless verification rejects the
		// witness, which is the assertion here.
		w := pbinWitnessOf(t, api, block)
		require.NotEmpty(t, w.State, "block %d", block)
	}

}
