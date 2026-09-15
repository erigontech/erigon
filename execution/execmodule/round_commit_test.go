package execmodule_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
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

// A round the caller has stopped waiting for runs on in the background, and its caller has already put its
// transactions back in the backlog. If it could still merge, those transactions would be in the block AND
// queued again. Commit and abandon are one claim — whoever takes it first decides — so a round that loses
// the claim leaves the block exactly as it was, and a round that wins it is committed even if its deadline
// passes while it merges.
func TestRoundCommitIsOneDecision(t *testing.T) {
	ctx := t.Context()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	// A second sender for the round that wins the claim: the first sender's abandoned nonce never
	// executed, so its next nonce would be a gap and filtered out before the claim was ever reached.
	otherKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	other := crypto.PubkeyToAddress(otherKey.PublicKey)

	funds := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: funds}, other: {Balance: funds}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
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
	sign := func(k *ecdsa.PrivateKey, to common.Address, nonce uint64) []byte {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, to, uint256.NewInt(1), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, k)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}

	_, _, _, err = exec.PreExecuteFlashblock(ctx, inputs, [][]byte{sign(key, sender, 0)})
	require.NoError(t, err)
	require.Len(t, exec.FlashBodyForTest(), 1)
	require.Equal(t, 1, exec.InProgressReceiptCount())

	// The caller took the claim first: the round must not commit.
	lost := execmodule.WithRoundCommit(ctx, func() bool { return false })
	_, _, _, err = exec.PreExecuteFlashblock(lost, inputs, [][]byte{sign(key, sender, 1)})
	require.ErrorIs(t, err, execmodule.ErrRoundAbandoned)
	require.Len(t, exec.FlashBodyForTest(), 1, "a round that lost the claim is still in the body")
	require.Equal(t, 1, exec.InProgressReceiptCount(), "a round that lost the claim merged its receipts")

	// The round took the claim, and its deadline passed the instant it did. It is committed all the same:
	// the caller saw the claim taken and is waiting for this result.
	wonCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	won := execmodule.WithRoundCommit(wonCtx, func() bool { cancel(); return true })
	_, _, vr, err := exec.PreExecuteFlashblock(won, inputs, [][]byte{sign(otherKey, other, 0)})
	require.NoError(t, err, "a round that won the claim was reported abandoned")
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)
	require.Len(t, exec.FlashBodyForTest(), 2)
	require.Equal(t, 2, exec.InProgressReceiptCount())
}
