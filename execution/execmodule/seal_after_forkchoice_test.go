package execmodule_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

// A fork choice between a block's last round and its seal must not leave the seal with nothing to close. The fork
// choice tears down the module context that holds the in-progress header and body; only a round's insert used to
// put them back, and the seal ran a round in front of itself to make sure one had. It no longer does — nothing that
// can stall may run before a seal — so the seal has to restore them itself, without executing. live43: the fork
// choice to genesis at boot landed between block 1's open and its seal, and the seal closed no block.
func TestSealAfterAForkChoiceStillClosesTheOpenBlock(t *testing.T) {
	ctx := t.Context()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	exec := m.ExecModule

	head, err := exec.CurrentHeader(ctx)
	require.NoError(t, err)
	var beaconRoot common.Hash
	params := &builder.Parameters{
		ParentHash:            head.Hash(),
		Timestamp:             head.Time + 1,
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &beaconRoot,
	}
	in := execmodule.FlashblockInputs{
		Parent:                head.Hash(),
		Number:                head.Number.Uint64() + 1,
		GasLimit:              head.GasLimit,
		BaseFee:               *misc.CalcBaseFee(m.ChainConfig, head),
		Timestamp:             params.Timestamp,
		ParentBeaconBlockRoot: beaconRoot,
		Withdrawals:           params.Withdrawals,
	}
	tx, err := types.SignTx(
		types.NewTransaction(0, sender, uint256.NewInt(1), 21_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))

	_, _, vr, err := exec.PreExecuteFlashblock(ctx, in, [][]byte{buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "round: %s", vr.ValidationError)

	// The consensus layer's fork choice for the parent lands before the seal.
	ur, err := updateForkChoice(ctx, exec, head)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status)

	br, err := exec.SealBlock(ctx, params, false)
	require.NoError(t, err)
	require.NotNil(t, br, "the seal closed no block after a fork choice")
	require.Len(t, br.Block.Transactions(), 1)
	require.Equal(t, tx.Hash(), br.Block.Transactions()[0].Hash())
	vr2, err := validateChain(ctx, exec, br.Block.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr2.ValidationStatus, "newPayload: %s", vr2.ValidationError)
}

// A block open under one slot's attributes and sealed under the next slot's is re-opened and its body replayed. The
// replay must still see the state the block started from. The abandoned generation's writes went into the shared
// state cache, and the re-open's candidate filter reads nonces from it before execution resets it — so the block's
// own transaction read its already-applied nonce and was dropped, and the close then failed "nonce too low"
// (live43, block 1).
func TestSealUnderNewAttributesReplaysTheBodyFromItsParentState(t *testing.T) {
	ctx := t.Context()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	cfg := *chain.AllProtocolChanges //nolint:govet // the tester wants a value, and this copy is never shared
	cfg.AmsterdamTime = nil
	genesis := &types.Genesis{
		Config: &cfg,
		Alloc:  types.GenesisAlloc{sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	exec := m.ExecModule

	head, err := exec.CurrentHeader(ctx)
	require.NoError(t, err)
	var beaconRoot common.Hash
	in := execmodule.FlashblockInputs{
		Parent:                head.Hash(),
		Number:                head.Number.Uint64() + 1,
		GasLimit:              head.GasLimit,
		BaseFee:               *misc.CalcBaseFee(m.ChainConfig, head),
		Timestamp:             head.Time + 1,
		ParentBeaconBlockRoot: beaconRoot,
		Withdrawals:           []*types.Withdrawal{},
	}
	tx, err := types.SignTx(
		types.NewTransaction(0, sender, uint256.NewInt(1), 21_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	_, _, vr, err := exec.PreExecuteFlashblock(ctx, in, [][]byte{buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "round: %s", vr.ValidationError)

	// The slot passed; the consensus layer asks for the same block under the next slot's attributes.
	later := &builder.Parameters{
		ParentHash:            head.Hash(),
		Timestamp:             head.Time + 3,
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &beaconRoot,
	}
	br, err := exec.SealBlock(ctx, later, false)
	require.NoError(t, err)
	require.NotNil(t, br)
	require.Equal(t, later.Timestamp, br.Block.Time())
	require.Len(t, br.Block.Transactions(), 1, "the re-open dropped the block's own transaction")
	vr2, err := validateChain(ctx, exec, br.Block.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr2.ValidationStatus, "newPayload: %s", vr2.ValidationError)
}
