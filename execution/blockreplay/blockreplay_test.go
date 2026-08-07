package blockreplay_test

import (
	"context"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/types"
)

// TestCaptureReplayRoundTrip pins the whole harness pipeline: capture a block's
// read-set + RLP from a populated datadir, serialize it (gob), reload it, replay
// it against an in-memory reader, and require that receipt/gas/bloom validation
// (inside ExecuteBlockEphemerally) still passes — i.e. the fixture is a faithful
// exec witness with no DB behind it.
func TestCaptureReplayRoundTrip(t *testing.T) {
	t.Parallel()

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keyAddr := crypto.PubkeyToAddress(key.PublicKey)
	keyFunds := new(big.Int).Mul(big.NewInt(1_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc:  types.GenesisAlloc{keyAddr: types.GenesisAccount{Balance: keyFunds}},
	}
	emt := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	signer := types.LatestSignerForChainID(emt.ChainConfig.ChainID)
	gen, err := blockgen.GenerateChain(emt.ChainConfig, emt.Genesis, emt.Engine, emt.DB, 6, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for j := range 3 {
			to := common.BytesToAddress([]byte{byte(i + 1), byte(j + 1), 0xab})
			txn, txErr := types.SignTx(
				types.NewTransaction(b.TxNonce(keyAddr), to, uint256.NewInt(1_000_000), params.TxGas, uint256.NewInt(1), nil),
				*signer, key)
			require.NoError(t, txErr)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, emt.InsertChain(gen))

	ctx := context.Background()
	logger := log.New()
	const target = uint64(4) // mid-chain: has txns, parent + ancestors present

	tx, err := emt.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	fx, err := blockreplay.Capture(ctx, tx, emt.BlockReader, emt.ChainConfig, emt.Engine, target, logger)
	require.NoError(t, err)
	require.NotEmpty(t, fx.BlockRLP)
	require.NotEmpty(t, fx.ParentHeaderRLP)
	require.NotEmpty(t, fx.Accounts)

	path := filepath.Join(t.TempDir(), "block.gob")
	require.NoError(t, fx.Save(path))
	loaded, err := blockreplay.Load(path)
	require.NoError(t, err)

	res, err := blockreplay.Replay(loaded, emt.ChainConfig, emt.Engine, 0, logger)
	require.NoError(t, err, "replay must reproduce receipts from the captured witness")

	want := gen.Blocks[target-1]
	require.Equal(t, want.ReceiptHash(), res.ReceiptRoot, "replayed receipt root must match block header")
}
