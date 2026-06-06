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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/generics"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

func TestValidateChainWithLastTxNumOfBlockAtStepBoundary(t *testing.T) {
	// See https://github.com/erigontech/erigon/issues/18823
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubKey := privKey.PublicKey
	senderAddr := crypto.PubkeyToAddress(pubKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	stepSize := uint64(5) // 2 for block 0 (0,1) and 3 for block 1 (2,3,4)
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithStepSize(stepSize),
	)
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(0, senderAddr, uint256.NewInt(0), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, 1)
	exec := m.ExecModule
	insertRes, err := insertBlocks(ctx, exec, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insertRes)
	validationReceipt, err := validateChain(ctx, exec, chainPack.Blocks[0].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validationReceipt.ValidationStatus)
	require.Equal(t, "", validationReceipt.ValidationError)
	extendingHash, extendingNum, extendingSd := m.ForkValidator.ExtendingFork()
	require.Equal(t, chainPack.Blocks[0].Hash(), extendingHash)
	require.Equal(t, uint64(1), extendingNum)
	var inMemBlockNum, inMemTxNum uint64
	err = m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		v, _, err := extendingSd.GetLatest(kv.CommitmentDomain, tx, commitmentdb.KeyCommitmentState)
		require.NoError(t, err)
		inMemTxNum = binary.BigEndian.Uint64(v[:8])
		inMemBlockNum = binary.BigEndian.Uint64(v[8:16])
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), inMemBlockNum)
	// previously the bug was that we stored the commitment state key as if it was at block 1 txNum 3
	// when it should have been at txNum 4
	require.Equal(t, uint64(4), inMemTxNum)
	root, err := extendingSd.GetCommitmentCtx().Trie().RootHash()
	require.NoError(t, err)
	require.Equal(t, chainPack.Headers[0].Root, common.BytesToHash(root))
}

func TestValidateChainAndUpdateForkChoiceWithSideForksThatGoBackAndForwardInHeight(t *testing.T) {
	// This was caught by some of the gas-benchmark tests which run a series of new payloads and FCUs
	// for forks with different lengths, and they jump from one fork to another.
	// The issue was that we were not calling TruncateCanonicalHash for heights after the new FCU head number.
	// Which meant that AppendCanonicalTxNums was appending more txNums than it should for the given FCU fork
	// (i.e. it was going beyond it) if the fork before it was longer in height. This caused a wrong trie root.
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubKey := privKey.PublicKey
	senderAddr := crypto.PubkeyToAddress(pubKey)
	privKey2, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubKey2 := privKey2.PublicKey
	senderAddr2 := crypto.PubkeyToAddress(pubKey2)
	privKey3, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr3 := crypto.PubkeyToAddress(privKey3.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr:  {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			senderAddr2: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			senderAddr2: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	longerFork, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	//goland:noinspection DuplicatedCode
	shorterFork, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr2, uint256.NewInt(2_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	//goland:noinspection DuplicatedCode
	longerFork2, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr3, uint256.NewInt(3_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	err = insertValidateAndUfc1By1(t.Context(), m.ExecModule, longerFork.Blocks)
	require.NoError(t, err)
	err = insertValidateAndUfc1By1(t.Context(), m.ExecModule, shorterFork.Blocks)
	require.NoError(t, err)
	err = insertValidateAndUfc1By1(t.Context(), m.ExecModule, longerFork2.Blocks)
	require.NoError(t, err)
}

// Regression for PR #21415: when state's commitBlock is ahead of TxNums.Last
// (e.g. after a snapshot/state misalignment + chaindata wipe), an FCU to a
// block beyond the canonical tip must not be rejected as ReorgTooDeep.
// The forkchoice should detect that unwindTarget equals the canonical tip,
// skip the unwind path, write the new canonicals, and let AppendCanonicalTxNums
// re-extend TxNums past commitBlock.
func TestUpdateForkChoiceRecoversWhenStateAheadOfTxNums(t *testing.T) {
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
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, 10)

	// Drive the full chain in (insert + validate + UFC). State commitBlock and
	// TxNums.Last are now both at block 10.
	require.NoError(t, insertValidateAndUfc1By1(ctx, m.ExecModule, chainPack.Blocks))

	const truncateTo uint64 = 5
	// Simulate the post-OtterSync state where chaindata's canonical+TxNums only
	// extend to a frozen tip but the snapshot/state files commit to a later
	// block. We truncate canonical+TxNums down to block 5 while leaving the
	// commitment domain at block 10, and clear the changeset table (which a
	// chaindata wipe would empty). With no changesets, CanUnwindToBlockNum
	// falls back to the commitment block (10) — so an unwind to block 5 looks
	// "too deep" and the pre-fix code rejects the FCU as ReorgTooDeep.
	var commitBlock uint64
	require.NoError(t, m.DB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		v, _, err := tx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(v), 16)
		commitBlock = binary.BigEndian.Uint64(v[8:16])
		require.NoError(t, rawdbv3.TxNums.Truncate(tx, truncateTo+1))
		require.NoError(t, rawdb.TruncateCanonicalHash(tx, truncateTo+1, false))
		require.NoError(t, tx.ClearTable(kv.ChangeSets3))
		return nil
	}))
	require.Equal(t, uint64(10), commitBlock, "commitBlock should be at block 10 after the initial chain")
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		lastBlock, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.Equal(t, truncateTo, lastBlock, "canonical/TxNums must be truncated below commitBlock to set up the misalignment")
		return nil
	}))

	// FCU on the same chain's tip (block 10). Walk-back finds canonical ancestor
	// at the truncated tip (block 5), unwindTarget = 5 = lastCanonicalBlock,
	// so the no-unwind branch fires. The handler then writes canonical entries
	// for blocks 6..10 and AppendCanonicalTxNums re-extends TxNums.
	//
	// Because the domain was ahead of blocks when the FCU started, the handler
	// returns ExecutionStatusTooFarAway ("domain ahead of blocks") rather than
	// ReorgTooDeep — a signal the CL (Caplin/Astrid/EngineServer) knows how to
	// handle by inserting the missing canonical blocks and retrying.
	res, err := updateForkChoice(ctx, m.ExecModule, chainPack.Blocks[len(chainPack.Blocks)-1].Header())
	require.NoError(t, err)
	require.NotEqual(t, execmodule.ExecutionStatusReorgTooDeep, res.Status, "must not be rejected as ReorgTooDeep")
	require.Equal(t, execmodule.ExecutionStatusTooFarAway, res.Status, "should signal domain-ahead-of-blocks so the CL retries")

	// After the FCU, TxNums must have caught up to (or past) commitBlock so
	// the next NewSharedDomains() will not return ErrBehindCommitment.
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		lastBlock, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, lastBlock, commitBlock, "TxNums.Last must have caught up to commitBlock")
		return nil
	}))
}

// Companion to TestUpdateForkChoiceRecoversWhenStateAheadOfTxNums that also
// exercises forward execution: state lags the chain tip (executed to block 10,
// chain extends to 15). The first FCU repairs the canonical/TxNums index and
// signals TooFarAway; the second FCU then drives execution forward 10 -> 15.
// The committed state at block 10 stays in the domain tables (the base for
// forward execution) — only the index tables are truncated.
func TestUpdateForkChoiceForwardExecutesAfterStateAheadRecovery(t *testing.T) {
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
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 15, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, 15)

	// Execute only the first 10 blocks: state/commitBlock/execProgress at 10.
	require.NoError(t, insertValidateAndUfc1By1(ctx, m.ExecModule, chainPack.Blocks[:10]))
	// Insert headers/bodies for 11..15 so the forkchoice walk-back can traverse
	// them, but do not UFC — they are neither canonical nor executed yet.
	insRes, err := insertBlocks(ctx, m.ExecModule, chainPack.Blocks[10:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)

	const truncateTo uint64 = 5
	var commitBlock uint64
	require.NoError(t, m.DB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		v, _, err := tx.GetLatest(kv.CommitmentDomain, commitmentdb.KeyCommitmentState)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(v), 16)
		commitBlock = binary.BigEndian.Uint64(v[8:16])
		require.NoError(t, rawdbv3.TxNums.Truncate(tx, truncateTo+1))
		require.NoError(t, rawdb.TruncateCanonicalHash(tx, truncateTo+1, false))
		require.NoError(t, tx.ClearTable(kv.ChangeSets3))
		return nil
	}))
	require.Equal(t, uint64(10), commitBlock)

	tip := chainPack.Blocks[len(chainPack.Blocks)-1].Header()

	// First FCU to the tip (block 15). Index is repaired (canonical + TxNums
	// extended to 15) and, since the domain was behind, we get TooFarAway.
	res, err := updateForkChoice(ctx, m.ExecModule, tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusTooFarAway, res.Status, "first FCU should repair index and signal domain-ahead")
	var execProgAfter1 uint64
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		lastBlock, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.Equal(t, uint64(15), lastBlock, "TxNums must be re-extended to the tip")
		execProgAfter1, err = stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		return nil
	}))
	require.Equal(t, commitBlock, execProgAfter1, "execution should not have advanced yet (state still at commitBlock)")

	// Second FCU to the same tip: domain is no longer ahead (TxNums caught up),
	// so execution is driven forward from commitBlock to the tip.
	res2, err := updateForkChoice(ctx, m.ExecModule, tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, res2.Status, "second FCU should execute forward to the tip")
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		execProg, err := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		require.Equal(t, uint64(15), execProg, "execution must have advanced to the tip")
		return nil
	}))
}

func addTwoTxnsToPool(ctx context.Context, startingNonce uint64, t *testing.T, m *execmoduletester.ExecModuleTester, txpool txpoolproto.TxpoolServer, baseFee uint64) {
	tx2, err := types.SignTx(types.NewTransaction(startingNonce, common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(baseFee), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)
	tx3, err := types.SignTx(types.NewTransaction(startingNonce+1, common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(baseFee), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)
	rlpTxs := make([][]byte, 2)
	for i, tx := range []types.Transaction{tx2, tx3} {
		var buf bytes.Buffer
		err = tx.EncodeRLP(&buf)
		require.NoError(t, err)
		rlpTxs[i] = buf.Bytes()
	}
	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{
		RlpTxs: rlpTxs,
	})
	require.NoError(t, err)
	require.Len(t, r.Errors, 2)
	for _, err := range r.Errors {
		require.Equal(t, "success", err)
	}
	require.Len(t, r.Imported, 2)
	for _, res := range r.Imported {
		require.Equal(t, txpoolproto.ImportResult_SUCCESS, res)
	}
}

func TestAssembleBlock(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		// In block 1, addr1 sends addr2 some ether.
		tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)
	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	addTwoTxnsToPool(ctx, 1, t, m, txpool, baseFee)

	var parentBeaconBlockRoot common.Hash
	_, err = rand.Read(parentBeaconBlockRoot[:])
	require.NoError(t, err)
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.NumberU64())
	require.Len(t, block.Transactions(), 2)

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

func TestAssembleBlockWithFreshlyAddedTxns(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		// In block 1, addr1 sends addr2 some ether.
		tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)
	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	addTwoTxnsToPool(ctx, 1, t, m, txpool, baseFee)

	var parentBeaconBlockRoot common.Hash
	_, err = rand.Read(parentBeaconBlockRoot[:])
	require.NoError(t, err)
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
	})
	require.NoError(t, err)

	// Add new transactions with a delay
	time.Sleep(300 * time.Millisecond)
	addTwoTxnsToPool(ctx, 3, t, m, txpool, baseFee)

	// The block should have all four transactions
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.NumberU64())
	require.Len(t, block.Transactions(), 4)

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

func insertBlocks(ctx context.Context, exec *execmodule.ExecModule, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
	rawBlocks := make([]*types.RawBlock, len(blocks))
	for i, b := range blocks {
		rawBlocks[i] = &types.RawBlock{Header: b.HeaderNoCopy(), Body: b.RawBody()}
	}
	return retryBusy(ctx, func() (execmodule.ExecutionStatus, bool, error) {
		status, err := exec.InsertBlocks(ctx, rawBlocks)
		if err != nil {
			return execmodule.ExecutionStatusBusy, false, err
		}
		return status, status == execmodule.ExecutionStatusBusy, nil
	})
}

func validateChain(ctx context.Context, exec *execmodule.ExecModule, h *types.Header) (execmodule.ValidationResult, error) {
	return retryBusy(ctx, func() (execmodule.ValidationResult, bool, error) {
		r, err := exec.ValidateChain(ctx, h.Hash(), h.Number.Uint64())
		if err != nil {
			return execmodule.ValidationResult{}, false, err
		}
		return r, r.ValidationStatus == execmodule.ExecutionStatusBusy, nil
	})
}

func updateForkChoice(ctx context.Context, exec *execmodule.ExecModule, h *types.Header) (execmodule.ForkChoiceResult, error) {
	return retryBusy(ctx, func() (execmodule.ForkChoiceResult, bool, error) {
		r, err := exec.UpdateForkChoice(ctx, h.Hash(), common.Hash{}, common.Hash{})
		if err != nil {
			return execmodule.ForkChoiceResult{}, false, err
		}
		return r, r.Status == execmodule.ExecutionStatusBusy, nil
	})
}

func insertValidateAndUfc1By1(ctx context.Context, exec *execmodule.ExecModule, blocks []*types.Block) error {
	ir, err := insertBlocks(ctx, exec, blocks)
	if err != nil {
		return err
	}
	if ir != execmodule.ExecutionStatusSuccess {
		return fmt.Errorf("unexpected insertBlocks status: %s", ir)
	}
	for _, b := range blocks {
		h := b.Header()
		vr, err := validateChain(ctx, exec, h)
		if err != nil {
			return err
		}
		if vr.ValidationStatus != execmodule.ExecutionStatusSuccess {
			return fmt.Errorf("unexpected validateChain status: %s", vr.ValidationStatus)
		}
		ur, err := updateForkChoice(ctx, exec, h)
		if err != nil {
			return err
		}
		if ur.Status != execmodule.ExecutionStatusSuccess {
			return fmt.Errorf("unexpected updateForkChoice status: %s", ur.Status)
		}
	}
	// UpdateForkChoice returns before the background flush+commit finishes
	// (per #21444). The next semaphore-acquiring op blocks until the prior
	// FCU's commit defers complete — so do one more idempotent FCU for the
	// last block to ensure commitBlock has settled before the caller reads it.
	if len(blocks) > 0 {
		if _, err := updateForkChoice(ctx, exec, blocks[len(blocks)-1].Header()); err != nil {
			return err
		}
	}
	return nil
}

func assembleBlock(ctx context.Context, exec *execmodule.ExecModule, params *builder.Parameters) (uint64, error) {
	return retryBusy(ctx, func() (uint64, bool, error) {
		r, err := exec.AssembleBlock(ctx, params)
		if err != nil {
			return 0, false, err
		}
		return r.PayloadID, r.Busy, nil
	})
}

func getAssembledBlock(ctx context.Context, exe *execmodule.ExecModule, payloadId uint64) (*types.Block, error) {
	return retryBusy(ctx, func() (*types.Block, bool, error) {
		r, err := exe.GetAssembledBlock(ctx, payloadId)
		if err != nil {
			return nil, false, err
		}
		if r.Block == nil {
			return nil, r.Busy, nil
		}
		return r.Block.Block, r.Busy, nil
	})
}

func retryBusy[T any](ctx context.Context, f func() (T, bool, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var b backoff.BackOff
	b = backoff.NewConstantBackOff(time.Millisecond)
	b = backoff.WithContext(b, ctx)
	return backoff.RetryWithData(
		func() (T, error) {
			r, busy, err := f()
			if err != nil {
				return generics.Zero[T](), backoff.Permanent(err) // no retries
			}
			if busy {
				return generics.Zero[T](), errors.New("retrying busy")
			}
			return r, nil
		},
		b,
	)
}

func randomHash() common.Hash {
	var h common.Hash
	_, _ = rand.Read(h[:])
	return h
}

func TestAssembleEmptyBlock(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule

	// Build 1 block with 1 tx as genesis state.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		tx, txErr := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Don't add any txns to pool — assemble empty block.
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)

	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.NumberU64())
	require.Empty(t, block.Transactions())

	// Insert + validate + FCU — validates state root is correct.
	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

func TestAssembleBlockWithStateVerification(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Build 1 block with 1 tx as genesis state.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		tx, txErr := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	addTwoTxnsToPool(ctx, 1, t, m, txpool, baseFee)

	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)

	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.NumberU64())
	require.Len(t, block.Transactions(), 2)

	// Validate the block and update fork choice.
	// insertValidateAndUfc1By1 verifies the state root is correct, which proves
	// that the assembled block's execution produced the exact expected state.
	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)

	// Build a second block (block 3) with 2 more txns to verify multi-block assembly.
	addTwoTxnsToPool(ctx, 3, t, m, txpool, baseFee)
	payloadId2, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            block.Hash(),
		Timestamp:             block.Header().Time + 1,
		PrevRandao:            block.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)

	block2, err := getAssembledBlock(ctx, exec, payloadId2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), block2.NumberU64())
	require.Len(t, block2.Transactions(), 2)

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block2})
	require.NoError(t, err)
}

func TestAssembleBlockWithContractCreation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Build 1 block with 1 tx as genesis state.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		tx, txErr := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Add a contract creation tx to the pool.
	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	changerBytecode, err := hex.DecodeString(contracts.ChangerBin[2:]) // strip "0x" prefix
	require.NoError(t, err)

	contractTx, err := types.SignTx(
		types.NewContractCreation(1, uint256.NewInt(0), 300_000, uint256.NewInt(baseFee), changerBytecode),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
	)
	require.NoError(t, err)

	var buf bytes.Buffer
	err = contractTx.EncodeRLP(&buf)
	require.NoError(t, err)
	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
	require.NoError(t, err)
	require.Len(t, r.Errors, 1)
	require.Equal(t, "success", r.Errors[0])

	// Assemble block.
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)

	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.NumberU64())
	require.Len(t, block.Transactions(), 1)

	// Insert + validate + FCU — validates state root which proves
	// contract deployment was executed correctly.
	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

func TestAssembleBlockGasOverflow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 150_000, // ~7 simple transfers at 21K gas
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithTxPool(),
	)
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Generate 1 empty block as initial chain state.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1,
		func(i int, gen *blockgen.BlockGen) {})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Add 10 txns to pool (each 21K gas, only ~7 fit per block).
	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	rlpTxs := make([][]byte, 10)
	for i := range rlpTxs {
		tx, txErr := types.SignTx(
			types.NewTransaction(uint64(i), common.Address{1}, uint256.NewInt(100),
				params.TxGas, uint256.NewInt(baseFee), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, txErr)
		var buf bytes.Buffer
		err = tx.EncodeRLP(&buf)
		require.NoError(t, err)
		rlpTxs[i] = buf.Bytes()
	}
	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, e := range r.Errors {
		require.Equal(t, "success", e)
	}

	// Assemble block 2 — should be gas-limited.
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	b2TxCount := len(block.Transactions())
	require.Greater(t, b2TxCount, 0)
	require.Less(t, b2TxCount, 10) // not all fit

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)

	// Assemble block 3 — remaining txns spill over.
	payloadId2, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            block.Hash(),
		Timestamp:             block.Header().Time + 1,
		PrevRandao:            block.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)
	block2, err := getAssembledBlock(ctx, exec, payloadId2)
	require.NoError(t, err)
	b3TxCount := len(block2.Transactions())
	require.Greater(t, b3TxCount, 0)

	// All 10 transactions should be included across the 2 blocks.
	require.Equal(t, 10, b2TxCount+b3TxCount)

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block2})
	require.NoError(t, err)
}

func TestAssembleBlockMixedTxTypes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Build 1 block with 1 tx as genesis state.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		tx, txErr := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	baseFee := chainPack.TopBlock.BaseFee().Uint64()

	// nonce 1: simple transfer
	tx1, err := types.SignTx(
		types.NewTransaction(1, common.Address{2}, uint256.NewInt(5_000), params.TxGas, uint256.NewInt(baseFee), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)

	// nonce 2: contract creation (Changer bytecode)
	changerBytecode, err := hex.DecodeString(contracts.ChangerBin[2:])
	require.NoError(t, err)
	tx2, err := types.SignTx(
		types.NewContractCreation(2, uint256.NewInt(0), 300_000, uint256.NewInt(baseFee), changerBytecode),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)

	// nonce 3: simple transfer
	tx3, err := types.SignTx(
		types.NewTransaction(3, common.Address{3}, uint256.NewInt(3_000), params.TxGas, uint256.NewInt(baseFee), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)

	// Add all 3 to pool.
	rlpTxs := make([][]byte, 3)
	for i, tx := range []types.Transaction{tx1, tx2, tx3} {
		var buf bytes.Buffer
		err = tx.EncodeRLP(&buf)
		require.NoError(t, err)
		rlpTxs[i] = buf.Bytes()
	}
	addR, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, e := range addR.Errors {
		require.Equal(t, "success", e)
	}

	// Assemble block.
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            chainPack.TopBlock.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: func() *common.Hash { h := randomHash(); return &h }(),
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 3)

	// Verify we have both transfer and contract creation tx types.
	hasTransfer := false
	hasContractCreation := false
	for _, tx := range block.Transactions() {
		if tx.GetTo() == nil {
			hasContractCreation = true
		} else {
			hasTransfer = true
		}
	}
	require.True(t, hasTransfer, "block should contain transfer transactions")
	require.True(t, hasContractCreation, "block should contain contract creation transaction")

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

// TestAssembleBlockWithWithdrawalRequest sends a withdrawal request transaction
// to the EIP-7002 system contract, builds a block via the real EL builder, and
// verifies execution requests are returned through ChainReaderWriterEth1.GetAssembledBlock
// — the exact interface Caplin uses in production (PR #14326 fixed this path).
// It then validates the block and extends the chain via insert + validate + FCU.
func TestAssembleBlockWithWithdrawalRequest(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Insert 1 initial block.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Submit withdrawal request transaction.
	var pubkey [48]byte
	for i := range pubkey {
		pubkey[i] = 0x02
	}
	var calldata []byte
	calldata = append(calldata, pubkey[:]...)
	calldata = append(calldata, make([]byte, 8)...) // amount=0

	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	withdrawalAddr := params.WithdrawalRequestAddress.Value()
	withdrawalTx, err := types.SignTx(
		&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 1_000_000,
				To:       &withdrawalAddr,
				Value:    *uint256.NewInt(500_000_000_000_000_000),
				Data:     calldata,
			},
			GasPrice: *uint256.NewInt(baseFee),
		},
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)

	var txBuf bytes.Buffer
	err = withdrawalTx.EncodeRLP(&txBuf)
	require.NoError(t, err)
	addResp, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{txBuf.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, "success", addResp.Errors[0])

	// Assemble block.
	beaconRoot := randomHash()
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            randomHash(),
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
	})
	require.NoError(t, err)

	// Get the assembled block via ChainReaderWriterEth1 — Caplin's production interface.
	chainRW := chainreader.NewChainReaderEth1(
		m.ChainConfig,
		exec,
		time.Hour,
	)

	eth1Block, blobsBundle, requestsBundle, blockValue, err := chainRW.GetAssembledBlock(payloadId)
	require.NoError(t, err)
	require.NotNil(t, eth1Block, "Eth1Block should not be nil")
	require.NotNil(t, blobsBundle, "BlobsBundle should not be nil")
	require.NotNil(t, blockValue, "blockValue should not be nil")

	// This is the critical assertion: the RequestsBundle must be returned.
	// PR #14326 added this return value. If reverted, this would be nil.
	require.NotNil(t, requestsBundle, "RequestsBundle must not be nil — "+
		"this is the return value added by PR #14326 to fix issue #14319")
	require.NotEmpty(t, requestsBundle.GetRequests(),
		"should contain at least one execution request")

	// Find and decode the withdrawal request.
	var foundWithdrawalRequest bool
	for _, req := range requestsBundle.GetRequests() {
		if len(req) == 0 || req[0] != types.WithdrawalRequestType {
			continue
		}
		requestData := req[1:]
		require.Equal(t, types.WithdrawalRequestDataLen, len(requestData))

		gotPubkey := requestData[20:68]
		require.Equal(t, pubkey[:], gotPubkey,
			"withdrawal request pubkey should match what was submitted")
		foundWithdrawalRequest = true
	}
	require.True(t, foundWithdrawalRequest,
		"should find a withdrawal request via ChainReaderWriterEth1.GetAssembledBlock")

	// Verify the block also passes validation.
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

// TestAssembleBlockAmsterdamForkTransition builds pre-Amsterdam blocks via the
// builder, then assembles the first Amsterdam block. This reproduces the
// Kurtosis failure from erigontech/erigon#20243 where the chain stalls at
// the Glamsterdam fork boundary.
func TestAssembleBlockAmsterdamForkTransition(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Amsterdam activates at timestamp 5 — blocks at t<5 are pre-Amsterdam,
	// the block at t=5 is the first Amsterdam block.
	amsterdamTime := uint64(5)
	cfg := &chain.Config{
		ChainID:                       uint256.NewInt(1337),
		Rules:                         chain.EtHashRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		MuirGlacierBlock:              common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		ArrowGlacierBlock:             common.NewUint64(0),
		GrayGlacierBlock:              common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		CancunTime:                    common.NewUint64(0),
		PragueTime:                    common.NewUint64(0),
		OsakaTime:                     common.NewUint64(0),
		AmsterdamTime:                 &amsterdamTime,
		DepositContract:               common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa"),
		Ethash:                        new(chain.EthashConfig),
	}

	m := execmoduletester.New(t,
		execmoduletester.WithTxPool(),
		execmoduletester.WithChainConfig(cfg),
		execmoduletester.WithExperimentalBAL(),
	)
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Build 3 pre-Amsterdam blocks via the builder (timestamps 1, 2, 3).
	topBlock := m.Genesis
	baseFee := topBlock.BaseFee().Uint64()
	for i := 0; i < 3; i++ {
		nonce := uint64(i)
		tx, txErr := types.SignTx(
			types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(baseFee), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, txErr)
		var buf bytes.Buffer
		require.NoError(t, tx.EncodeRLP(&buf))
		r, addErr := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
		require.NoError(t, addErr)
		require.Equal(t, "success", r.Errors[0])

		ts := topBlock.Header().Time + 1
		require.Less(t, ts, amsterdamTime) // still pre-Amsterdam
		var prevRandao, beaconRoot common.Hash
		_, _ = rand.Read(prevRandao[:])
		_, _ = rand.Read(beaconRoot[:])
		payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
			ParentHash:            topBlock.Hash(),
			Timestamp:             ts,
			PrevRandao:            prevRandao,
			SuggestedFeeRecipient: common.Address{1},
			Withdrawals:           make([]*types.Withdrawal, 0),
			ParentBeaconBlockRoot: &beaconRoot,
		})
		require.NoError(t, err)

		block, err := getAssembledBlock(ctx, exec, payloadId)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), block.NumberU64())

		err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
		require.NoError(t, err)
		topBlock = block
		baseFee = block.BaseFee().Uint64()
	}

	require.Less(t, topBlock.Header().Time, amsterdamTime, "pre-Amsterdam blocks should have timestamp < amsterdamTime")

	// Add a contract deployment tx to the pool — exercises state gas (EIP-8037)
	// more than simple EOA transfers.
	deployCode := []byte{0x60, 0x00} // PUSH1 0x00 — minimal contract
	deployTx, err := types.SignTx(
		types.NewContractCreation(3, uint256.NewInt(0), 1_000_000, uint256.NewInt(baseFee), deployCode),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
	)
	require.NoError(t, err)
	var deployBuf bytes.Buffer
	require.NoError(t, deployTx.EncodeRLP(&deployBuf))
	addResp, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{deployBuf.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, "success", addResp.Errors[0])

	// Also add a simple transfer.
	simpleTx, err := types.SignTx(
		types.NewTransaction(4, common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(baseFee), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
	)
	require.NoError(t, err)
	var simpleBuf bytes.Buffer
	require.NoError(t, simpleTx.EncodeRLP(&simpleBuf))
	addResp2, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{simpleBuf.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, "success", addResp2.Errors[0])

	// Now assemble the first Amsterdam block (timestamp = amsterdamTime).
	slotNumber := uint64(8) // mimics Kurtosis: epoch 1 slot 8 with minimal preset
	var amsPrevRandao, amsBeaconRoot common.Hash
	_, _ = rand.Read(amsPrevRandao[:])
	_, _ = rand.Read(amsBeaconRoot[:])
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            topBlock.Hash(),
		Timestamp:             amsterdamTime,
		PrevRandao:            amsPrevRandao,
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &amsBeaconRoot,
		SlotNumber:            &slotNumber,
	})
	require.NoError(t, err)

	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)
	require.NotNil(t, block, "first Amsterdam block should be built successfully")
	require.True(t, cfg.IsAmsterdam(block.Header().Time), "block should be an Amsterdam block")
	require.GreaterOrEqual(t, len(block.Transactions()), 1, "block should contain txpool txns")

	// Insert, validate, and update fork choice.
	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

// TestNotificationDispatchForegroundCommit verifies that after FCU returns
// Success with the default foreground commit path:
// 1. Header notifications have been dispatched (subscribers receive them)
// 2. Block data is committed to DB (eth_getBlockByNumber works)
func TestNotificationDispatchForegroundCommit(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t)
	exec := m.ExecModule

	// Subscribe to header notifications before any blocks.
	headerCh, unsub := m.Notifications.Events.AddHeaderSubscription()
	defer unsub()

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, nil)
	require.NoError(t, err)

	err = insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks)
	require.NoError(t, err)

	// After FCU returns Success, header notifications must already be
	// dispatched. Use a short timeout — they should be available immediately.
	drainHeaders(t, headerCh, 2*time.Second)

	// With foreground commit (default), data must be in the DB immediately
	// after FCU returns — this is what eth_getBlockByNumber relies on.
	err = m.DB.View(ctx, func(tx kv.Tx) error {
		for _, b := range chainPack.Blocks {
			h := rawdb.ReadHeader(tx, b.Hash(), b.NumberU64())
			require.NotNil(t, h, "block %d header should be in DB after foreground FCU", b.NumberU64())
		}
		return nil
	})
	require.NoError(t, err)
}

// TestNotificationDispatchBackgroundCommit verifies that with background
// commit enabled, notifications are still dispatched before FCU returns,
// even though the DB commit happens asynchronously.
//
// Note: with background commit, subsequent blocks may fail validation
// because the DB state hasn't caught up yet (the commit is async). This
// test only processes the genesis → block 1 transition to verify that
// notification dispatch works correctly in the background commit path.
func TestNotificationDispatchBackgroundCommit(t *testing.T) {
	// Background commit creates a race: FCU N returns before commit finishes,
	// so FCU N+1 reads stale state from DB. This is the known limitation that
	// the API-layer "latest head pointer" coordination is designed to solve.
	// Once that's implemented, remove this skip and verify the full flow.
	t.Skip("background commit requires API-layer coordination (latest head pointer) to work correctly")

	m := execmoduletester.New(t, execmoduletester.WithFcuBackgroundCommit())

	headerCh, unsub := m.Notifications.Events.AddHeaderSubscription()
	defer unsub()

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, nil)
	require.NoError(t, err)

	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	drainHeaders(t, headerCh, 2*time.Second)
}

// TestNotificationDispatchBackgroundPrune verifies that with the default
// production configuration (foreground commit + background prune), notifications
// are dispatched and data is committed before FCU returns. This was a real bug:
// background prune was incorrectly pulling the commit into the background
// goroutine too, causing eth_getBlockByNumber to return null after FCU Success.
func TestNotificationDispatchBackgroundPrune(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithFcuBackgroundPrune())
	exec := m.ExecModule

	headerCh, unsub := m.Notifications.Events.AddHeaderSubscription()
	defer unsub()

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, nil)
	require.NoError(t, err)

	err = insertValidateAndUfc1By1(ctx, exec, chainPack.Blocks)
	require.NoError(t, err)

	// Notifications dispatched.
	drainHeaders(t, headerCh, 2*time.Second)

	// Data committed to DB (prune is background, but commit is foreground).
	err = m.DB.View(ctx, func(tx kv.Tx) error {
		for _, b := range chainPack.Blocks {
			h := rawdb.ReadHeader(tx, b.Hash(), b.NumberU64())
			require.NotNil(t, h, "block %d header should be in DB with background prune", b.NumberU64())
		}
		return nil
	})
	require.NoError(t, err)
}

// drainHeaders reads from the header subscription channel until at least one
// header is received, or fails the test after timeout.
func drainHeaders(t *testing.T, ch <-chan [][]byte, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var count int
	for count == 0 {
		select {
		case hdrs := <-ch:
			count += len(hdrs)
		case <-timer.C:
			t.Fatal("timed out waiting for header notifications")
		}
	}
	require.Greater(t, count, 0, "should have received header notifications")
}

// TestAssembleBlockStateGasLimit verifies that the builder respects the EIP-8037
// block validity invariant: gas_used = max(regular, state) <= gas_limit.
//
// Contract creations have high intrinsic state gas (~184K per create at
// CostPerStateByte=1530, STATE_BYTES_PER_NEW_ACCOUNT=120) but low regular gas
// (~30K). With a 500K gas limit, about 3 creates would push state gas past
// the limit even though regular gas has room. Without the fix the builder
// would produce an invalid block.
func TestAssembleBlockStateGasLimit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 500_000, // low limit so state gas from a few creates exceeds it
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithTxPool(),
	)
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Generate 1 empty block as initial chain state.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1,
		func(i int, gen *blockgen.BlockGen) {})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Submit 10 contract creation txns to the pool.
	// Each has ~131K intrinsic state gas but only ~30K regular gas.
	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	deployCode := []byte{0x60, 0x00} // PUSH1 0x00 — minimal contract
	rlpTxs := make([][]byte, 10)
	for i := range rlpTxs {
		tx, txErr := types.SignTx(
			types.NewContractCreation(uint64(i), uint256.NewInt(0), 300_000, uint256.NewInt(baseFee), deployCode),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), privKey,
		)
		require.NoError(t, txErr)
		var buf bytes.Buffer
		require.NoError(t, tx.EncodeRLP(&buf))
		rlpTxs[i] = buf.Bytes()
	}
	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, e := range r.Errors {
		require.Equal(t, "success", e)
	}

	// Assemble block — builder must stop before state gas exceeds gas limit.
	slotNumber := uint64(1)
	parentBeaconBlockRoot := randomHash()
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            randomHash(),
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
		SlotNumber:            &slotNumber,
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)

	// The block must include some but not all 10 creates (state gas limited).
	txCount := len(block.Transactions())
	require.Greater(t, txCount, 0, "block should contain at least one tx")
	require.Less(t, txCount, 10, "builder should stop before all 10 creates fit")

	// EIP-8037 invariant: gas_used <= gas_limit.
	require.LessOrEqual(t, block.GasUsed(), block.GasLimit(),
		"gas_used (max of regular, state) must not exceed gas_limit")

	// Block must pass full validation (insert + validate + FCU).
	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

// TestAssembleBlockStateGasLimitSSTORE verifies the EIP-8037 block validity
// invariant for execution-time state gas (SSTOREs), as opposed to intrinsic
// state gas (contract creations tested above).
//
// A deployed contract writes 4 new storage slots per call (~150K execution
// state gas, ~41K regular gas, 0 intrinsic state gas). The txpool cannot
// filter these by state gas — only the check inside applyTransaction
// (between ApplyMessage and FinalizeTx) prevents the block from exceeding
// gas_limit.
func TestAssembleBlockStateGasLimitSSTORE(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 500_000,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithTxPool(),
	)
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	// Deploy a contract whose runtime writes to 4 storage slots per call.
	// Runtime: base = calldataload(0); sstore(base+i, 1) for i in 0..3.
	deployCode, err := hex.DecodeString(
		"601d600c600039601d6000f3" + // initcode: deploy 29-byte runtime
			"6000356001815560018160010155600181600201556001816003015500") // runtime
	require.NoError(t, err)

	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := m.Genesis.BaseFee().Uint64()
	deployTx, err := types.SignTx(
		types.NewContractCreation(0, uint256.NewInt(0), 300_000, uint256.NewInt(baseFee), deployCode),
		signer, privKey,
	)
	require.NoError(t, err)
	contractAddr := types.CreateAddress(senderAddr, 0)

	// Generate block 1 with the deployment.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1,
		func(i int, gen *blockgen.BlockGen) { gen.AddTx(deployTx) })
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Submit 10 call txns. Each writes 4 new slots (~150K state gas, ~41K
	// regular gas). With a 500K gas limit, 3 calls fit (~451K state gas)
	// but the 4th would push to ~601K. Intrinsic state gas is 0 for all
	// calls, so the txpool's regular-gas filter lets them all through —
	// the applyTransaction check is the only defense.
	baseFee = chainPack.TopBlock.BaseFee().Uint64()
	rlpTxs := make([][]byte, 10)
	for i := range rlpTxs {
		var calldata [32]byte
		binary.BigEndian.PutUint64(calldata[24:], uint64(i*4))
		tx, txErr := types.SignTx(
			types.NewTransaction(uint64(i+1), contractAddr, uint256.NewInt(0), 300_000, uint256.NewInt(baseFee), calldata[:]),
			signer, privKey,
		)
		require.NoError(t, txErr)
		var buf bytes.Buffer
		require.NoError(t, tx.EncodeRLP(&buf))
		rlpTxs[i] = buf.Bytes()
	}
	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, e := range r.Errors {
		require.Equal(t, "success", e)
	}

	// Assemble block 2.
	slotNumber := uint64(1)
	parentBeaconBlockRoot := randomHash()
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            randomHash(),
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
		SlotNumber:            &slotNumber,
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)

	txCount := len(block.Transactions())
	require.Greater(t, txCount, 0, "block should contain at least one tx")
	require.Less(t, txCount, 10, "builder should stop before all 10 calls fit")

	// EIP-8037 invariant: gas_used <= gas_limit.
	require.LessOrEqual(t, block.GasUsed(), block.GasLimit(),
		"gas_used (max of regular, state) must not exceed gas_limit")

	err = insertValidateAndUfc1By1(ctx, exec, []*types.Block{block})
	require.NoError(t, err)
}

// TestAssembleBlockGasPoolSnapshotRestoreBug exercises the per-tx gas pool
// snapshot/restore in the block assembler's commitTx path. Under EIP-8037 the
// pool tracks regular and state gas as separate dimensions, so a restore that
// only captures the regular dimension and seeds both on restore wrongly
// inflates the state pool, letting a follow-up tx exceed the block's
// state-gas limit.
//
// The scenario relies on a tx whose intrinsic state gas (the part the txpool
// can see when filtering) fits in the remaining pool but whose total state
// gas (intrinsic + on-success code-deposit) does not: such a tx passes the
// txpool's filter, reaches the assembler, and fails ConsumeState inside
// ApplyTransaction. The bug then surfaces if a successor tx in the same
// batch consumes the inflated state pool that the restore wrongly handed
// back.
//
// Fresh senders (each at nonce 0) are required so a failed tx doesn't
// nonce-block its successors within the batch.
func TestAssembleBlockGasPoolSnapshotRestoreBug(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Initcode that deploys a 100-byte runtime (100 zero bytes) via CODECOPY.
	// Per byte deployed: 1530 state gas (CPSB) + 200 regular gas. So each
	// CREATE consumes ~153K state gas on top of the 183.6K intrinsic
	// NEW_ACCOUNT charge — a total of ~337K state per tx.
	const runtimeLen = 100
	initHeader := []byte{
		0x60, byte(runtimeLen), // PUSH1 length
		0x60, 0x0c, //             PUSH1 12 (runtime offset in initcode)
		0x60, 0x00, //             PUSH1 0  (memory destination)
		0x39,                   // CODECOPY
		0x60, byte(runtimeLen), // PUSH1 length
		0x60, 0x00, //             PUSH1 0 (memory offset)
		0xf3, //                   RETURN
	}
	deployCode := append(initHeader, make([]byte, runtimeLen)...)

	// With a 1_000_000 block gas limit, two txs (~337K state each) leave
	// the pool at ~326K; a third has 184K intrinsic state (fits in pool by
	// txpool's filter) but needs 337K total (fails ConsumeState during
	// execution). A fourth tx then succeeds against the wrongly inflated
	// pool, pushing the block's total state gas past gas_limit.
	const numSenders = 4
	keys := make([]*ecdsa.PrivateKey, numSenders)
	addrs := make([]common.Address, numSenders)
	for i := range keys {
		k, err := crypto.GenerateKey()
		require.NoError(t, err)
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(k.PublicKey)
	}

	alloc := types.GenesisAlloc{}
	for _, a := range addrs {
		alloc[a] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}
	}
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 1_000_000,
		Alloc:    alloc,
	}

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(keys[0]),
		execmoduletester.WithTxPool(),
	)
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1,
		func(i int, gen *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := chainPack.TopBlock.BaseFee().Uint64()

	rlpTxs := make([][]byte, numSenders)
	for i, k := range keys {
		tx, err := types.SignTx(
			types.NewContractCreation(0, uint256.NewInt(0), 400_000, uint256.NewInt(baseFee), deployCode),
			signer, k,
		)
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, tx.EncodeRLP(&buf))
		rlpTxs[i] = buf.Bytes()
	}
	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, e := range r.Errors {
		require.Equal(t, "success", e)
	}

	slotNumber := uint64(1)
	parentBeaconBlockRoot := randomHash()
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            randomHash(),
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
		SlotNumber:            &slotNumber,
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)

	require.Greater(t, len(block.Transactions()), 0, "block should contain at least one tx")
	require.LessOrEqual(t, block.GasUsed(), block.GasLimit(),
		"gas_used (max of regular, state) must not exceed gas_limit")

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, []*types.Block{block}))
}

// TestAssembleBlockGasPoolMultiBatchInitBug exercises the block assembler's
// per-batch gas-pool initialisation. The block builder calls AddTransactions
// repeatedly with batches of up to 50 txs from the txpool. Each call must
// build the pool with the *per-dimension* remaining budget; seeding both
// dimensions from the regular-only cumulative gas wrongly inflates the state
// pool when state gas has run ahead of regular gas after the previous batch,
// letting a tx in the next batch consume state past gas_limit.
//
// The scenario: 50 contract creations in batch 1 push cumulative state gas
// near the block gas limit while keeping cumulative regular gas low (CREATE
// has ~184K intrinsic state vs ~30K intrinsic regular per tx). The 51st tx
// has small intrinsic state (so the txpool's state-aware filter admits it
// into batch 2) but a large code-deposit state on execution. With the pool
// init seeded from regular gas only, batch 2 starts with a state pool that
// matches the regular dimension — i.e. far more than the real remaining
// state budget — and the 51st tx is wrongly accepted.
func TestAssembleBlockGasPoolMultiBatchInitBug(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// 50 zero-deposit CREATE txs (initcode returns nothing) at higher gas
	// price for batch 1, plus 1 large-deposit CREATE at lower gas price so
	// the txpool orders it into batch 2.
	const numBatch1 = 50
	const numTotal = numBatch1 + 1
	keys := make([]*ecdsa.PrivateKey, numTotal)
	addrs := make([]common.Address, numTotal)
	for i := range keys {
		k, err := crypto.GenerateKey()
		require.NoError(t, err)
		keys[i] = k
		addrs[i] = crypto.PubkeyToAddress(k.PublicKey)
	}

	alloc := types.GenesisAlloc{}
	for _, a := range addrs {
		alloc[a] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}
	}
	// 10M block limit fits ~54 CREATEs by intrinsic state (184K each ≈ 9.2M
	// for 50). Leaves ~800K of state headroom for batch 2, which the trigger
	// tx's ~1.2M total state exceeds.
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 10_000_000,
		Alloc:    alloc,
	}

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(keys[0]),
		execmoduletester.WithTxPool(),
	)
	exec := m.ExecModule
	txpool := m.TxPoolGrpcServer

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1,
		func(i int, gen *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	highPrice := uint256.NewInt(baseFee * 2) // higher tip → batch 1
	lowPrice := uint256.NewInt(baseFee)      // baseFee only → batch 2

	// Batch 1 initcode: PUSH1 0, PUSH1 0, RETURN → zero-byte runtime, no
	// code-deposit state. Per-tx state is just the 184K intrinsic.
	batch1Init := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}

	// Batch 2 initcode deploys a ~660-byte runtime via CODECOPY. Per-byte
	// deposit cost: 1530 state + 200 regular. ~660 bytes → ~1M state on top
	// of the 184K intrinsic.
	const triggerRuntimeLen = 660
	triggerInit := []byte{
		0x61, byte(triggerRuntimeLen >> 8), byte(triggerRuntimeLen & 0xff), // PUSH2 length
		0x60, 0x0d, //             PUSH1 13 (runtime offset)
		0x60, 0x00, //             PUSH1 0  (memory dest)
		0x39,                                                               //                   CODECOPY
		0x61, byte(triggerRuntimeLen >> 8), byte(triggerRuntimeLen & 0xff), // PUSH2 length
		0x60, 0x00, //             PUSH1 0  (memory offset)
		0xf3, //                   RETURN
	}
	triggerInit = append(triggerInit, make([]byte, triggerRuntimeLen)...)

	rlpTxs := make([][]byte, numTotal)
	for i := range numBatch1 {
		tx, txErr := types.SignTx(
			types.NewContractCreation(0, uint256.NewInt(0), 250_000, highPrice, batch1Init),
			signer, keys[i],
		)
		require.NoError(t, txErr)
		var buf bytes.Buffer
		require.NoError(t, tx.EncodeRLP(&buf))
		rlpTxs[i] = buf.Bytes()
	}
	triggerTx, err := types.SignTx(
		types.NewContractCreation(0, uint256.NewInt(0), 2_000_000, lowPrice, triggerInit),
		signer, keys[numBatch1],
	)
	require.NoError(t, err)
	var triggerBuf bytes.Buffer
	require.NoError(t, triggerTx.EncodeRLP(&triggerBuf))
	rlpTxs[numBatch1] = triggerBuf.Bytes()

	r, err := txpool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, e := range r.Errors {
		require.Equal(t, "success", e)
	}

	slotNumber := uint64(1)
	parentBeaconBlockRoot := randomHash()
	payloadId, err := assembleBlock(ctx, exec, &builder.Parameters{
		ParentHash:            chainPack.TopBlock.Hash(),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            randomHash(),
		SuggestedFeeRecipient: common.Address{1},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
		SlotNumber:            &slotNumber,
	})
	require.NoError(t, err)
	block, err := getAssembledBlock(ctx, exec, payloadId)
	require.NoError(t, err)

	require.Greater(t, len(block.Transactions()), 0, "block should contain at least one tx")
	require.LessOrEqual(t, block.GasUsed(), block.GasLimit(),
		"gas_used (max of regular, state) must not exceed gas_limit")

	require.NoError(t, insertValidateAndUfc1By1(ctx, exec, []*types.Block{block}))
}

func TestEIP7708BurnLogWhenCoinbaseSelfDestructs(t *testing.T) {
	// Regression test for https://github.com/erigontech/erigon/issues/19951
	//
	// When the coinbase is a contract that self-destructs during execution,
	// EIP-7708 requires a Burn log for the residual balance (priority fee)
	// credited after the SELFDESTRUCT. Post-EIP-6780 SELFDESTRUCT only
	// deletes contracts created in the same transaction, so we CREATE a
	// contract at the pre-computed coinbase address whose init code
	// immediately SELFDESTRUCTs to the caller.
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
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2 // non-zero priority fee
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)

	// Init code: CALLER (0x33) SELFDESTRUCT (0xFF).
	// Creates a contract that immediately self-destructs, sending any
	// balance to the transaction sender. Post-EIP-6780 this deletes the
	// contract because it was created in the same transaction.
	initCode := []byte{0x33, 0xFF}

	var coinbaseAddr common.Address
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		// Pre-compute the CREATE address — the contract will be deployed here.
		coinbaseAddr = types.CreateAddress(senderAddr, nonce)
		gen.SetCoinbase(coinbaseAddr)

		tx, txErr := types.SignTx(
			types.NewContractCreation(nonce, uint256.NewInt(0), 300_000, uint256.NewInt(gasPrice), initCode),
			*signer,
			privKey,
		)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)

	// Verify the receipt contains an EIP-7708 Burn log.
	require.Len(t, chainPack.Receipts[0], 1)
	receipt := chainPack.Receipts[0][0]
	require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
	require.Greater(t, receipt.GasUsed, uint64(0))

	var burnLog *types.Log
	var burnCount, transferCount int
	for _, log := range receipt.Logs {
		if log.Address != params.SystemAddress.Value() || len(log.Topics) < 2 {
			continue
		}
		switch log.Topics[0] {
		case misc.EthBurnLogEvent:
			burnLog = log
			burnCount++
		case misc.EthTransferLogEvent:
			transferCount++
		}
	}
	require.Equal(t, 1, burnCount, "expected exactly one EIP-7708 Burn log")
	require.Equal(t, 0, transferCount, "no Transfer log expected for zero-value CREATE")
	require.Equal(t, coinbaseAddr.Hash(), burnLog.Topics[1],
		"burn log should reference the coinbase address")

	// Burnt amount = priority fee = gasUsed × effectiveTip.
	// Use the actual block baseFee (EIP-1559 adjusts it from genesis).
	blockBaseFee := chainPack.Headers[0].BaseFee.Uint64()
	expectedBurn := new(uint256.Int).Mul(
		uint256.NewInt(receipt.GasUsed),
		uint256.NewInt(gasPrice-blockBaseFee),
	)
	burnBytes := expectedBurn.Bytes32()
	require.Equal(t, burnBytes[:], []byte(burnLog.Data),
		"burn amount should equal the priority fee credited to coinbase")

	// Insert + validate + FCU proves the state root is computed correctly.
	err = insertValidateAndUfc1By1(ctx, m.ExecModule, chainPack.Blocks)
	require.NoError(t, err)
}

// TestInsertBlocksWithBatchedFCU drives the Caplin persistent_block_collector
// pattern: InsertBlocks(batch) → ForkChoiceUpdate(last block of batch),
// repeated for each batch. Verifies parent TD continuity across batches —
// the "parent's total difficulty not found" failure mode the collector
// comment warns about does not actually occur on the catch-up path.
func TestInsertBlocksWithBatchedFCU(t *testing.T) {
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

	const totalBlocks = 30
	const batchSize = 10

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, totalBlocks, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, totalBlocks)

	for start := 0; start < totalBlocks; start += batchSize {
		end := start + batchSize
		batch := chainPack.Blocks[start:end]

		insRes, err := insertBlocks(ctx, m.ExecModule, batch)
		require.NoError(t, err, "batch [%d..%d] InsertBlocks", start+1, end)
		require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)

		last := batch[len(batch)-1].Header()
		fcuRes, err := updateForkChoice(ctx, m.ExecModule, last)
		require.NoError(t, err, "batch [%d..%d] FCU on block %d", start+1, end, last.Number.Uint64())
		require.Equal(t, execmodule.ExecutionStatusSuccess, fcuRes.Status,
			"FCU on block %d should succeed; validationError=%q", last.Number.Uint64(), fcuRes.ValidationError)

		// After each batch's FCU, TxNums + execution must have advanced to
		// the batch tip. The next batch's first block reads its parent's TD
		// from this committed state.
		require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
			lastTxNumBlock, _, err := rawdbv3.TxNums.Last(tx)
			require.NoError(t, err)
			require.Equal(t, last.Number.Uint64(), lastTxNumBlock, "TxNums.Last after batch ending at %d", last.Number.Uint64())

			execProg, err := stages.GetStageProgress(tx, stages.Execution)
			require.NoError(t, err)
			require.Equal(t, last.Number.Uint64(), execProg, "Execution stage progress after batch ending at %d", last.Number.Uint64())

			// First block of the just-inserted batch should have a readable TD
			// (asserts the rawdb.WriteTd in InsertBlocks made it to DB).
			firstOfBatch := batch[0].HeaderNoCopy()
			td, err := rawdb.ReadTd(tx, firstOfBatch.Hash(), firstOfBatch.Number.Uint64())
			require.NoError(t, err)
			require.NotNil(t, td, "TD for block %d must be readable across the batch boundary", firstOfBatch.Number.Uint64())
			return nil
		}))
	}
}

// runBatchedFCUBadBlockRecovery is the shared body for the foreground- and
// background-commit variants of the bad-block recovery test. Both FCU
// cleanup branches — local SD close (foreground) and the additional
// currentContext reset (background) — must leave the next InsertBlocks+FCU
// cycle able to recover.
func runBatchedFCUBadBlockRecovery(t *testing.T, bgCommit bool) {
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
	opts := []execmoduletester.Option{
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	}
	if bgCommit {
		opts = append(opts, execmoduletester.WithFcuBackgroundCommit())
	}
	m := execmoduletester.New(t, opts...)

	// Under background commit, a commit (including the genesis InsertBlocks inside
	// New) lands asynchronously after the call returns. These polls let DB reads
	// wait for the commit goroutine; both return immediately under foreground
	// commit. Transient read errors are treated as "not ready yet" and retried.
	waitForGenesis := func() {
		require.Eventually(t, func() bool {
			var funded bool
			err := m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
				v, _, err := tx.GetLatest(kv.AccountsDomain, senderAddr[:])
				if err != nil {
					return err
				}
				funded = len(v) > 0
				return nil
			})
			return err == nil && funded
		}, 15*time.Second, 10*time.Millisecond, "genesis commit did not land")
	}
	waitForBlock := func(blockNum uint64) {
		require.Eventually(t, func() bool {
			var done bool
			err := m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
				last, _, err := rawdbv3.TxNums.Last(tx)
				if err != nil {
					return err
				}
				done = last >= blockNum
				return nil
			})
			return err == nil && done
		}, 15*time.Second, 10*time.Millisecond, "commit of block %d did not land", blockNum)
	}
	// Genesis must be committed before GenerateChain reads its state.
	waitForGenesis()

	// Build a 6-block chain. First 5 are committed normally; block 6 is the
	// recovery target.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 6, func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), senderAddr, uint256.NewInt(1_000), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(nil),
			privKey,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	})
	require.NoError(t, err)
	require.Len(t, chainPack.Blocks, 6)

	// Phase 1: insert + FCU blocks 1..5 normally so state is at 5.
	insRes, err := insertBlocks(ctx, m.ExecModule, chainPack.Blocks[:5])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	res, err := updateForkChoice(ctx, m.ExecModule, chainPack.Blocks[4].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, res.Status)
	waitForBlock(5)

	// Phase 2: forge a "bad block 6" sharing the canonical parent (block 5's
	// hash) but with a corrupted state root. InsertBlocks accepts it
	// (header-write only); FCU on it triggers execution which computes the
	// real post-state root, finds the mismatch and returns BadBlock.
	goodHeader6 := chainPack.Blocks[5].HeaderNoCopy()
	badHeader6 := types.CopyHeader(goodHeader6)
	badHeader6.Root = common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	badBlock6 := types.NewBlockFromNetwork(badHeader6, &types.Body{
		Transactions: chainPack.Blocks[5].Transactions(),
		Uncles:       chainPack.Blocks[5].Uncles(),
		Withdrawals:  chainPack.Blocks[5].Withdrawals(),
	})

	badRes, err := insertBlocks(ctx, m.ExecModule, []*types.Block{badBlock6})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, badRes, "InsertBlocks writes the header without validating state root")

	fcuBad, err := updateForkChoice(ctx, m.ExecModule, badBlock6.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, fcuBad.Status,
		"FCU on the bad block must return BadBlock (state-root mismatch); got %s validationError=%q", fcuBad.Status, fcuBad.ValidationError)

	// Phase 3: recovery — insert the real block 6 and FCU on it. This must
	// succeed even though the bad-block FCU just closed the local
	// SharedDomains. The persistent e.currentContext is either still
	// pointing to the prior SD (foreground) or has been nil'd (background);
	// both paths must let the next InsertBlocks re-initialize the overlay
	// cleanly.
	recoverIns, err := insertBlocks(ctx, m.ExecModule, []*types.Block{chainPack.Blocks[5]})
	require.NoError(t, err, "InsertBlocks of the good block after a bad-block FCU must not error")
	require.Equal(t, execmodule.ExecutionStatusSuccess, recoverIns)

	recoverFcu, err := updateForkChoice(ctx, m.ExecModule, chainPack.Blocks[5].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, recoverFcu.Status,
		"FCU on the good block after recovery must succeed; validationError=%q", recoverFcu.ValidationError)
	waitForBlock(6)

	// State must be at the good block 6 — TxNums.Last and Execution progress
	// both at 6, parent TD readable.
	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		lastTxNumBlock, _, err := rawdbv3.TxNums.Last(tx)
		require.NoError(t, err)
		require.Equal(t, uint64(6), lastTxNumBlock)

		execProg, err := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		require.Equal(t, uint64(6), execProg)

		td, err := rawdb.ReadTd(tx, chainPack.Blocks[5].Hash(), 6)
		require.NoError(t, err)
		require.NotNil(t, td)
		return nil
	}))
}

// TestInsertBlocksWithBatchedFCU_BadBlockRecovery_Foreground covers the
// foreground-commit cleanup path: a bad-block FCU closes the local
// SharedDomains while the persistent currentContext remains, and the next
// InsertBlocks must re-initialize the overlay on top of it.
func TestInsertBlocksWithBatchedFCU_BadBlockRecovery_Foreground(t *testing.T) {
	runBatchedFCUBadBlockRecovery(t, false)
}

// TestInsertBlocksWithBatchedFCU_BadBlockRecovery_Background covers the
// background-commit cleanup path, where the bad-block FCU additionally resets
// currentContext. Commits land asynchronously, so the shared body polls
// committed state before asserting (see waitForGenesis/waitForBlock).
func TestInsertBlocksWithBatchedFCU_BadBlockRecovery_Background(t *testing.T) {
	runBatchedFCUBadBlockRecovery(t, true)
}

// transferGen returns a deterministic per-block tx generator: identical
// inputs produce identical blocks, which lets tests build forks that share
// a prefix with the canonical chain (requires a pre-Cancun config — Cancun+
// blocks get a random ParentBeaconBlockRoot in blockgen).
func transferGen(t *testing.T, key *ecdsa.PrivateKey, to common.Address, amount uint64) func(int, *blockgen.BlockGen) {
	return func(i int, b *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(uint64(i), to, uint256.NewInt(amount), 50000, uint256.NewInt(1), nil),
			*types.LatestSignerForChainID(nil),
			key,
		)
		require.NoError(t, err)
		b.AddTx(tx)
	}
}

// Regression for #21650: a batch longer than MaxReorgDepth must still produce
// changesets for the last MaxReorgDepth blocks, otherwise no reorg of any
// depth is possible after the batch.
func TestLargeBatchExecGeneratesChangesetsForReorgWindow(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	m := execmoduletester.New(t, execmoduletester.WithKey(privKey))

	maxReorgDepth := m.Cfg().Sync.MaxReorgDepth
	chainLen := int(maxReorgDepth) + 14

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLen,
		transferGen(t, privKey, senderAddr, 1_000))
	require.NoError(t, err)

	insRes, err := insertBlocks(ctx, m.ExecModule, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	fcuRes, err := updateForkChoice(ctx, m.ExecModule, chainPack.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcuRes.Status)

	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		lowest, err := changeset.ReadLowestUnwindableBlock(tx)
		require.NoError(t, err)
		require.Equal(t, uint64(chainLen)-maxReorgDepth, lowest,
			"changesets must cover the last MaxReorgDepth blocks of the batch")
		return nil
	}))
}

// Reproduces #21650 end-to-end: after a single batch execution longer than
// MaxReorgDepth, an FCU onto a fork branching a few blocks below the tip must
// unwind and re-execute instead of failing with ReorgTooDeep.
func TestUpdateForkChoiceShallowReorgAfterLargeBatchExec(t *testing.T) {
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	m := execmoduletester.New(t, execmoduletester.WithKey(privKey))

	maxReorgDepth := m.Cfg().Sync.MaxReorgDepth
	chainLen := int(maxReorgDepth) + 14
	const reorgDepth = 4
	divergeFrom := chainLen - reorgDepth

	canonical, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLen,
		transferGen(t, privKey, senderAddr, 1_000))
	require.NoError(t, err)
	fork, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLen,
		func(i int, b *blockgen.BlockGen) {
			amount := uint64(1_000)
			if i >= divergeFrom {
				amount = 2_000
			}
			transferGen(t, privKey, senderAddr, amount)(i, b)
		})
	require.NoError(t, err)
	require.Equal(t, canonical.Blocks[divergeFrom-1].Hash(), fork.Blocks[divergeFrom-1].Hash())
	require.NotEqual(t, canonical.Blocks[divergeFrom].Hash(), fork.Blocks[divergeFrom].Hash())

	insRes, err := insertBlocks(ctx, m.ExecModule, canonical.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	fcuRes, err := updateForkChoice(ctx, m.ExecModule, canonical.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcuRes.Status)

	insRes, err = insertBlocks(ctx, m.ExecModule, fork.Blocks[divergeFrom:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)
	fcuRes, err = updateForkChoice(ctx, m.ExecModule, fork.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcuRes.Status,
		"shallow reorg of %d blocks after a %d-block batch must succeed; status=%s validationError=%q",
		reorgDepth, chainLen, fcuRes.Status, fcuRes.ValidationError)

	require.NoError(t, m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		execProg, err := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		require.Equal(t, uint64(chainLen), execProg)
		require.Equal(t, fork.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
		return nil
	}))
}
