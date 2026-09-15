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
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
)

// A transaction the producer names as slot-exceeded is sealed into the block as FAILED — whole gas limit used,
// nonce consumed — with the verdict carried in the header's extra-data, and the block validates when replayed
// from that header: the verdict, not a clock, decides the outcome on every node.
func TestSlotExceededTxIsSealedAsFailedAndReplays(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privKey.PublicKey)

	cfg := *chain.AllProtocolChanges
	cfg.AmsterdamTime = nil
	cfg.SlotExceededTxs = true

	genesis := &types.Genesis{
		Config: &cfg,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	exec := m.ExecModule

	const gasLimit = 50_000
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	sign := func(nonce uint64) ([]byte, common.Hash) {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, sender, uint256.NewInt(1), gasLimit, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, privKey)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes(), tx.Hash()
	}

	produce := func(parent *types.Header, nonces []uint64, named map[uint64]bool) *types.BlockWithReceipts {
		var beaconRoot common.Hash
		timestamp := parent.Time + 1
		params := &builder.Parameters{
			ParentHash:            parent.Hash(),
			Timestamp:             timestamp,
			Withdrawals:           []*types.Withdrawal{},
			ParentBeaconBlockRoot: &beaconRoot,
		}
		inputs := execmodule.FlashblockInputs{
			Parent:                parent.Hash(),
			Number:                parent.Number.Uint64() + 1,
			GasLimit:              parent.GasLimit,
			BaseFee:               *misc.CalcBaseFee(m.ChainConfig, parent),
			Timestamp:             timestamp,
			ParentBeaconBlockRoot: beaconRoot,
			Withdrawals:           params.Withdrawals,
		}
		rlps := make([][]byte, 0, len(nonces))
		for _, n := range nonces {
			rlp, hash := sign(n)
			rlps = append(rlps, rlp)
			if named[n] {
				inputs.SlotExceeded = append(inputs.SlotExceeded, hash)
			}
		}
		_, _, vr, perr := exec.PreExecuteFlashblock(ctx, inputs, rlps)
		require.NoError(t, perr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "pre-exec: %s", vr.ValidationError)

		br, serr := exec.SealBlock(ctx, params, false)
		require.NoError(t, serr)
		require.NotNil(t, br)
		require.Len(t, br.Block.Transactions(), len(nonces))

		vr2, verr := validateChain(ctx, exec, br.Block.Header())
		require.NoError(t, verr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr2.ValidationStatus, "replay: %s", vr2.ValidationError)
		ur, uerr := updateForkChoice(ctx, exec, br.Block.Header())
		require.NoError(t, uerr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status)
		return br
	}

	head, err := exec.CurrentHeader(ctx)
	require.NoError(t, err)

	br := produce(head, []uint64{0, 1, 2}, map[uint64]bool{1: true})
	require.Equal(t, []uint16{1}, protocol.SlotExceededIndices(m.ChainConfig, br.Block.Header().Extra),
		"the sealed header names the body position of the slot-exceeded transaction")
	require.Len(t, br.Receipts, 3)
	require.Equal(t, types.ReceiptStatusSuccessful, br.Receipts[0].Status)
	require.Equal(t, types.ReceiptStatusFailed, br.Receipts[1].Status, "the named transaction is included as failed")
	require.Equal(t, uint64(gasLimit), br.Receipts[1].GasUsed, "and uses its whole gas limit")
	require.Equal(t, types.ReceiptStatusSuccessful, br.Receipts[2].Status, "the transactions after it are unaffected")

	// The named transaction consumed its nonce: the next block continues from 3.
	next := produce(br.Block.Header(), []uint64{3}, nil)
	require.Empty(t, next.Block.Header().Extra, "a block with nothing named carries no verdict")
	require.Equal(t, types.ReceiptStatusSuccessful, next.Receipts[0].Status)
}
