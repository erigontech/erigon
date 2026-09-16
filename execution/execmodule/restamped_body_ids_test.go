package execmodule_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

// A RE-STAMPED block must still hold ITS OWN transactions after the next block is produced.
//
// A withdrawals re-stamp re-hashes a block whose body has already been written: only WithdrawalsHash changes,
// but that changes the header hash. writePreExecBlock then calls WriteRawBodyIfNotExists, whose existence
// check is on (number, hash) — so under the new hash it MISSES, falls through to WriteRawBody, and
// IncrementSequence(kv.EthTx, ...) hands the block a SECOND BaseTxnID range, rewriting every transaction into
// it.
//
// That allocation is not durable from here. As ingestSealedFlashblockLocked already records for the seal:
// "the sequence the NEXT block is seeded from comes from the committed DB and does not carry it. The successor
// was therefore handed this block's ids and OVERWROTE its transactions: the sealed block read back a mixture
// of its own body and its successor's, so it could not be re-executed ('nonce too high' on receipt
// derivation) even though its state was correct."
//
// The seal was fixed for exactly this by RE-KEYING the in-progress body record instead of writing a second
// copy (TestSealedBodyKeepsItsOwnTransactionsAfterTheNextBlock pins that half). The re-stamp is the OTHER path
// that re-hashes a block whose body is already written, and it never got the same treatment.
//
// READ TIMING MATTERS, and getting it wrong makes this test measure nothing: a sealed block's body is not
// visible through GetBody until a later flush — measured, block 1 reads 0 transactions right after its own
// seal and 1 after the successor is produced, because GetBody resolves through e.currentContext's overlay
// (beginOverlayOrRo), the canonical module context, not the pre-exec generation it was sealed in. So the
// expected transaction is captured at SIGNING time, and the only assertion is taken after the successor —
// which is exactly where the overwrite this hazard predicts would show.
func TestRestampedBlockKeepsItsOwnTransactionsAfterTheNextBlock(t *testing.T) {
	h := newRoundHarness(t)
	ctx := h.ctx

	// The harness signs deterministically, so the same inputs reproduce the transaction block 1 will carry.
	expected := func(key int, nonce uint64) types.Transaction {
		k := h.keys[key]
		txn, err := types.SignTx(
			types.NewTransaction(nonce, crypto.PubkeyToAddress(k.PublicKey), uint256.NewInt(1), 50_000,
				uint256.NewInt(h.m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(h.m.ChainConfig.ChainID), k)
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, txn.MarshalBinary(&buf))
		return txn
	}
	wantTx := expected(0, 0)

	head, err := h.m.ExecModule.CurrentHeader(ctx)
	require.NoError(t, err)

	// Block 1: opened under an EMPTY withdrawals list, one transaction executed into it.
	params, in := h.attrs(head)
	require.NoError(t, h.round(ctx, in, 0, 0), "block 1 round")

	// The CL asks for the same block under a DIFFERENT withdrawals list. Every execution-affecting attribute
	// is unchanged, so this takes the re-stamp path: the header is rewritten and the block re-hashes.
	params.Withdrawals = []*types.Withdrawal{{
		Index:     1,
		Validator: 1,
		Address:   crypto.PubkeyToAddress(h.keys[1].PublicKey),
		Amount:    1,
	}}
	first := h.sealAndAdopt(params, 1)
	hash, number := first.Hash(), first.Number.Uint64()

	// Producing the SUCCESSOR is what re-seeds kv.EthTx from committed state; a second, non-durable id range
	// handed to block 1 is overwritten here.
	params2, in2 := h.attrs(first)
	require.NoError(t, h.round(ctx, in2, 1, 0), "block 2 round")
	h.sealAndAdopt(params2, 1)

	body, err := h.m.ExecModule.GetBody(ctx, &hash, &number)
	require.NoError(t, err)
	require.NotNil(t, body, "the re-stamped block must still have a body after its successor")
	require.Len(t, body.Transactions, 1,
		"the re-stamped block must hold exactly its own transactions after the next block is produced")

	gotTx, err := types.DecodeTransaction(body.Transactions[0])
	require.NoError(t, err)
	require.Equal(t, wantTx.Hash(), gotTx.Hash(),
		"block %d read back a transaction that is not the one it sealed — its body ids were reallocated by the "+
			"re-stamp and then overwritten by its successor", number)
}
