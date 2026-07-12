// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execmodule_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// tamperBlockGasUsed returns a copy of the block whose header claims the wrong
// gasUsed. The transactions stay valid, so execution succeeds and the block
// fails only the post-execution gas check — after its writes have already
// landed in the in-RAM overlay.
func tamperBlockGasUsed(t *testing.T, b *types.Block, delta uint64) *types.Block {
	header := b.Header()
	header.GasUsed += delta
	bad := types.NewBlockFromStorage(header.Hash(), header, b.Transactions(), b.Uncles(), b.Withdrawals())
	require.NotEqual(t, b.Hash(), bad.Hash(), "tampering must change the block hash")
	return bad
}

// TestUpdateForkChoiceBadBlockMidBatchThenRecovery feeds the module blocks it
// has never validated (as a CL applying devp2p-fetched blocks would) where one
// fails its post-execution gas check — once as the second block of the batch
// and once as the first. In both shapes the bad block must be rejected without
// condemning its valid ancestors or poisoning any reused state (overlay,
// caches, markers): the CL's recovery move — the untampered sibling at the
// same height — must validate and become head, and per-block accounting must
// come out exact.
func TestUpdateForkChoiceBadBlockMidBatchThenRecovery(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	const chainLen = 13
	const committedTo = 10
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLen, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, chainLen)

	require.NoError(t, insertValidateAndUfc1By1(ctx, m.ExecModule, chainPack.Blocks[:committedTo]))

	block11 := chainPack.Blocks[committedTo]
	block12 := chainPack.Blocks[committedTo+1]
	block12Bad := tamperBlockGasUsed(t, block12, 21_000)

	insRes, err := insertBlocks(ctx, m.ExecModule, []*types.Block{block11, block12Bad})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)

	// FCU straight to the bad tip: blocks 11 and 12-bad execute in one batch.
	// 12-bad must be rejected — and 11 must remain the latest valid block. If
	// the retry loop re-executes 11 against its own stale overlay writes, 11 is
	// wrongly reported bad and LatestValidHash degrades to block 10.
	res, err := updateForkChoice(ctx, m.ExecModule, block12Bad.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, res.Status, "tampered block must be rejected")
	t.Logf("bad-tip FCU: latestValidHash=%s (block10=%s block11=%s)", res.LatestValidHash, chainPack.Blocks[committedTo-1].Hash(), block11.Hash())
	// The module conservatively reports the last committed block (10), not the
	// deepest valid block of the failed batch (11) — accept either, never the
	// tampered block or anything outside its valid ancestry.
	require.Contains(t, []common.Hash{chainPack.Blocks[committedTo-1].Hash(), block11.Hash()}, res.LatestValidHash,
		"latest valid hash must be a valid ancestor of the rejected tip")

	// The CL's recovery move: switch to the untampered block 12 at the same
	// height. This only works if block 11 was not wrongly marked bad by the
	// in-loop retry — a bad-mark on 11 poisons every chain built on it.
	insRes, err = insertBlocks(ctx, m.ExecModule, []*types.Block{block12})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	vr, err := validateChain(ctx, m.ExecModule, block12.Header())
	require.NoError(t, err)
	require.Equalf(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus,
		"newPayload-style validation of the untampered block 12 must succeed (status=%s validationErr=%q latestValid=%s)",
		vr.ValidationStatus, vr.ValidationError, vr.LatestValidHash)
	res, err = updateForkChoice(ctx, m.ExecModule, block12.Header())
	require.NoError(t, err)
	require.Equalf(t, execmodule.ExecutionStatusSuccess, res.Status,
		"recovery FCU to the untampered block 12 must succeed (status=%s validationErr=%q latestValid=%s)",
		res.Status, res.ValidationError, res.LatestValidHash)
	// Idempotent settle FCU: the previous FCU may commit in the background.
	_, err = updateForkChoice(ctx, m.ExecModule, block12.Header())
	require.NoError(t, err)

	// Second shape: the bad block is the first block above committed progress,
	// so there is nothing on disk to roll back when it fails — any leftover of
	// its execution must die with the failed forkchoice, or the good block at
	// the same height re-reads the failed block's writes and is wrongly bad.
	block13 := chainPack.Blocks[committedTo+2]
	block13Bad := tamperBlockGasUsed(t, block13, 21_000)
	insRes, err = insertBlocks(ctx, m.ExecModule, []*types.Block{block13Bad})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	res, err = updateForkChoice(ctx, m.ExecModule, block13Bad.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, res.Status, "tampered first-of-batch block must be rejected")
	require.Equal(t, block12.Hash(), res.LatestValidHash, "latest valid hash must be the bad block's parent, the committed head")

	insRes, err = insertBlocks(ctx, m.ExecModule, []*types.Block{block13})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	vr, err = validateChain(ctx, m.ExecModule, block13.Header())
	require.NoError(t, err)
	require.Equalf(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus,
		"validation of the untampered block 13 must succeed (status=%s validationErr=%q)", vr.ValidationStatus, vr.ValidationError)
	res, err = updateForkChoice(ctx, m.ExecModule, block13.Header())
	require.NoError(t, err)
	require.Equalf(t, execmodule.ExecutionStatusSuccess, res.Status,
		"recovery FCU to the untampered block 13 must succeed (status=%s validationErr=%q latestValid=%s)",
		res.Status, res.ValidationError, res.LatestValidHash)
	_, err = updateForkChoice(ctx, m.ExecModule, block13.Header())
	require.NoError(t, err)

	var acc accounts.Account
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		v, _, err := tx.GetLatest(kv.AccountsDomain, senderAddr[:])
		require.NoError(t, err)
		require.NotEmpty(t, v)
		return accounts.DeserialiseV3(&acc, v)
	}))
	require.Equal(t, uint64(chainLen), acc.Nonce, "sender must have executed exactly one txn per block")
}

// TestUpdateForkChoiceBadBlockAtLongBatchTailThenRecovery drives one FCU over
// a long never-validated segment whose tail block is tampered — the devp2p
// catch-up shape of the same failure. The valid segment blocks executed into
// the same sync run as the failing tail; none of their effects may survive
// the failed forkchoice, or the recovery below (the untampered sibling at the
// bad height, then one more block) re-reads stale writes and is wrongly
// condemned.
func TestUpdateForkChoiceBadBlockAtLongBatchTailThenRecovery(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	const chainLen = 35
	const committedTo = 26
	const badHeight = 35 // tail of the never-validated segment (index badHeight-1)
	mkTx := func(i int, value uint64) types.Transaction {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(value), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		return tx
	}
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLen+1, func(i int, b *blockgen.BlockGen) {
		b.AddTx(mkTx(i, 1_000))
	})
	require.NoError(t, err)
	require.NoError(t, insertValidateAndUfc1By1(ctx, m.ExecModule, chainPack.Blocks[:committedTo]))

	segment := make([]*types.Block, 0, badHeight-committedTo)
	segment = append(segment, chainPack.Blocks[committedTo:badHeight-1]...)
	badTip := tamperBlockGasUsed(t, chainPack.Blocks[badHeight-1], 21_000)
	segment = append(segment, badTip)
	insRes, err := insertBlocks(ctx, m.ExecModule, segment)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)

	res, err := updateForkChoice(ctx, m.ExecModule, badTip.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, res.Status, "tampered segment tip must be rejected")
	t.Logf("bad-tip FCU: latestValidHash=%s (committed=%s lastGood=%s)",
		res.LatestValidHash, chainPack.Blocks[committedTo-1].Hash(), chainPack.Blocks[badHeight-2].Hash())
	validAncestors := make([]common.Hash, 0, badHeight-committedTo)
	for _, b := range chainPack.Blocks[committedTo-1 : badHeight-1] {
		validAncestors = append(validAncestors, b.Hash())
	}
	require.Contains(t, validAncestors, res.LatestValidHash,
		"latest valid hash must be a valid ancestor of the rejected tip")

	// Recovery via the untampered sibling at the bad height, then one more
	// block on top: only possible if the valid segment blocks were not
	// condemned during the failed FCU.
	recovery := []*types.Block{chainPack.Blocks[badHeight-1], chainPack.Blocks[badHeight]}
	insRes, err = insertBlocks(ctx, m.ExecModule, recovery)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	for _, b := range recovery {
		vr, err := validateChain(ctx, m.ExecModule, b.Header())
		require.NoError(t, err)
		require.Equalf(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus,
			"validation of recovery block %d must succeed (status=%s validationErr=%q)",
			b.NumberU64(), vr.ValidationStatus, vr.ValidationError)
	}
	forkTip := recovery[len(recovery)-1]
	res, err = updateForkChoice(ctx, m.ExecModule, forkTip.Header())
	require.NoError(t, err)
	require.Equalf(t, execmodule.ExecutionStatusSuccess, res.Status,
		"recovery FCU past the bad height must succeed (status=%s validationErr=%q latestValid=%s)",
		res.Status, res.ValidationError, res.LatestValidHash)
	_, err = updateForkChoice(ctx, m.ExecModule, forkTip.Header())
	require.NoError(t, err)

	var acc accounts.Account
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		v, _, err := tx.GetLatest(kv.AccountsDomain, senderAddr[:])
		require.NoError(t, err)
		require.NotEmpty(t, v)
		return accounts.DeserialiseV3(&acc, v)
	}))
	require.Equal(t, uint64(badHeight+1), acc.Nonce, "one txn per block through the recovery fork tip")
}
