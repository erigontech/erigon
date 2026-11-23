package execmodule_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

// A node whose build of a block ran past the block's end rebuilds it from the prefix the block holds, and the
// transactions it drops are then free to execute again in whatever comes next.
func TestRebuildFlashblockKeepsOnlyTheGivenBodyAndItsState(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privKey.PublicKey)

	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	exec := m.ExecModule

	parent := m.Genesis.HeaderNoCopy()
	inputs := execmodule.FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(m.ChainConfig, parent),
		Timestamp:             parent.Time + 1,
		ParentBeaconBlockRoot: common.Hash{0x01},
	}
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	signRLP := func(nonce uint64) []byte {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, sender, uint256.NewInt(1), 21_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, privKey)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}

	for n := uint64(0); n < 3; n++ {
		_, _, vr, rerr := exec.PreExecuteFlashblock(ctx, inputs, [][]byte{signRLP(n)})
		require.NoError(t, rerr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	}
	require.Len(t, exec.FlashBodyForTest(), 3)

	vr, err := exec.RebuildFlashblock(ctx, inputs, [][]byte{signRLP(0)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	require.Equal(t, [][]byte{signRLP(0)}, exec.FlashBodyForTest())

	_, _, vr, err = exec.PreExecuteFlashblock(ctx, inputs, [][]byte{signRLP(1)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	require.Equal(t, [][]byte{signRLP(0), signRLP(1)}, exec.FlashBodyForTest(), "a dropped transaction executes again after the rebuild")
}
