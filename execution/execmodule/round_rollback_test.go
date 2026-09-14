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

// A round joins the maintained body BEFORE it executes — the header the round builds and inserts has to
// carry its transactions. So a round that does not go on to commit has to come back out again.
//
// It did not. The body kept the failed round's transactions, and because the seal reads that body, the
// block sealed transactions its state knew nothing about. The same path is what the deadline valve relies
// on: dropping an overrunning round is only free if the block is left exactly as it was before the round
// began.
func TestFailedRoundLeavesTheBodyUntouched(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privKey.PublicKey)

	// The flashblock header builder produces no block access list, so this path runs pre-Amsterdam.
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	exec := m.ExecModule

	parent := m.Genesis.HeaderNoCopy()
	beaconRoot := common.Hash{0x01}
	inputs := execmodule.FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(m.ChainConfig, parent),
		Timestamp:             parent.Time + 1,
		ParentBeaconBlockRoot: beaconRoot,
	}

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	signRLP := func(nonce, gas uint64) []byte {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, sender, uint256.NewInt(1), gas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, privKey)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}

	// One good round, so the block has a body to be left alone.
	_, _, vr, err := exec.PreExecuteFlashblock(ctx, inputs, [][]byte{signRLP(0, 50_000)})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	good := exec.FlashBodyForTest()
	require.Len(t, good, 1)

	// A round that cannot execute: its transaction alone asks for more gas than the block has.
	_, _, _, err = exec.PreExecuteFlashblock(ctx, inputs, [][]byte{signRLP(1, parent.GasLimit+1)})
	require.Error(t, err, "a round that cannot execute must report failure")

	require.Equal(t, good, exec.FlashBodyForTest(),
		"the failed round is still in the body — the block would seal transactions that never executed")
}
