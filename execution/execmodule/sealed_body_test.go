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

// A sealed block must still hold ITS OWN transactions after the next block is produced.
//
// The seal used to write the sealed body with WriteRawBody, which allocates a fresh BaseTxnID range from the
// kv.EthTx sequence. That allocation was not durable — the sequence the next block gets seeded from comes
// from the committed DB and did not carry it — so the successor was handed the same ids and overwrote this
// block's transactions in kv.EthTx. The block then read back a mixture of its own body and its successor's:
// state correct, body unexecutable, and every receipt/log query or follower re-execution failed on it while
// the load harnesses reported a clean pass. The seal now RE-KEYS the in-progress body record instead.
func TestSealedBodyKeepsItsOwnTransactionsAfterTheNextBlock(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privKey.PublicKey)

	// The flashblock header builder does not produce a block access list, so the L2 config this path runs
	// under predates Amsterdam. AllProtocolChanges enables it, which would fail the block on "missing bal hash".
	cfg := *chain.AllProtocolChanges
	cfg.AmsterdamTime = nil

	genesis := &types.Genesis{
		Config: &cfg,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)
	exec := m.ExecModule

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	signRLP := func(nonce uint64) []byte {
		tx, serr := types.SignTx(
			types.NewTransaction(nonce, sender, uint256.NewInt(1), 50_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*signer, privKey)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		return buf.Bytes()
	}

	// produce accumulates nonces into a block on parent and seals it, exactly as the DAG driver does:
	// PreExecuteFlashblock feeds the body, SealBlock closes it under the same attributes.
	produce := func(parent *types.Header, timestamp uint64, nonces []uint64) *types.Block {
		var beaconRoot common.Hash
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
			rlps = append(rlps, signRLP(n))
		}
		_, _, vr, perr := exec.PreExecuteFlashblock(ctx, inputs, rlps)
		require.NoError(t, perr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, "pre-exec: %s", vr.ValidationError)

		br, serr := exec.SealBlock(ctx, params, false)
		require.NoError(t, serr)
		require.NotNil(t, br, "SealBlock must seal the pre-executed block")
		require.Len(t, br.Block.Transactions(), len(nonces))

		// newPayload + FCU, as the driver does. This is the step that makes the bug reachable: the commit is
		// what re-seeds the next block's kv.EthTx sequence from durable state.
		vr2, verr := validateChain(ctx, exec, br.Block.Header())
		require.NoError(t, verr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr2.ValidationStatus, "validate: %s", vr2.ValidationError)
		ur, uerr := updateForkChoice(ctx, exec, br.Block.Header())
		require.NoError(t, uerr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status)
		return br.Block
	}

	head, err := exec.CurrentHeader(ctx)
	require.NoError(t, err)
	require.NotNil(t, head)

	first := produce(head, head.Time+1, []uint64{0, 1, 2})
	// Producing the SUCCESSOR is what used to destroy the first block's body.
	produce(first.Header(), first.Header().Time+1, []uint64{3, 4})

	hash, number := first.Hash(), first.NumberU64()
	body, err := exec.GetBody(ctx, &hash, &number)
	require.NoError(t, err)
	require.NotNil(t, body, "the sealed block must still have a body")
	require.Len(t, body.Transactions, 3, "the sealed block must hold exactly its own transactions")

	for i, want := range first.Transactions() {
		got, derr := types.DecodeTransaction(body.Transactions[i])
		require.NoError(t, derr)
		require.Equal(t, want.Hash(), got.Hash(),
			"transaction %d of block %d is not the one it sealed", i, number)
	}
}
