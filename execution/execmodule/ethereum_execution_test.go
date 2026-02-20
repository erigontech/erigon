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
	"crypto/rand"
	"encoding/binary"
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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	eth1utils "github.com/erigontech/erigon/execution/execmodule/moduleutil"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestValidateChainWithLastTxNumOfBlockAtStepBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	m := execmoduletester.NewWithGenesis(t, genesis, privKey, execmoduletester.WithStepSize(stepSize))
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
	exec := m.Eth1ExecutionService
	insertRes, err := insertBlocks(ctx, exec, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, executionproto.ExecutionStatus_Success, insertRes.Result)
	validationReceipt, err := validateChain(ctx, exec, chainPack.Blocks[0].Header())
	require.NoError(t, err)
	require.Equal(t, executionproto.ExecutionStatus_Success, validationReceipt.ValidationStatus)
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	m := execmoduletester.NewWithGenesis(t, genesis, privKey)
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
	err = insertValidateAndUfc1By1(t.Context(), m.Eth1ExecutionService, longerFork.Blocks)
	require.NoError(t, err)
	err = insertValidateAndUfc1By1(t.Context(), m.Eth1ExecutionService, shorterFork.Blocks)
	require.NoError(t, err)
	err = insertValidateAndUfc1By1(t.Context(), m.Eth1ExecutionService, longerFork2.Blocks)
	require.NoError(t, err)
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.NewWithTxPoolAllProtocolChanges(t)
	exec := m.Eth1ExecutionService
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
	payloadId, err := assembleBlock(ctx, exec, &executionproto.AssembleBlockRequest{
		ParentHash:            gointerfaces.ConvertHashToH256(chainPack.TopBlock.Hash()),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            gointerfaces.ConvertHashToH256(chainPack.TopBlock.Header().MixDigest),
		SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(common.Address{1}),
		Withdrawals:           make([]*typesproto.Withdrawal, 0),
		ParentBeaconBlockRoot: gointerfaces.ConvertHashToH256(parentBeaconBlockRoot),
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
		t.Skip("skipping test in short mode")
	}
	t.Parallel()
	ctx := t.Context()
	m := execmoduletester.NewWithTxPoolAllProtocolChanges(t)
	exec := m.Eth1ExecutionService
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
	payloadId, err := assembleBlock(ctx, exec, &executionproto.AssembleBlockRequest{
		ParentHash:            gointerfaces.ConvertHashToH256(chainPack.TopBlock.Hash()),
		Timestamp:             chainPack.TopBlock.Header().Time + 1,
		PrevRandao:            gointerfaces.ConvertHashToH256(chainPack.TopBlock.Header().MixDigest),
		SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(common.Address{1}),
		Withdrawals:           make([]*typesproto.Withdrawal, 0),
		ParentBeaconBlockRoot: gointerfaces.ConvertHashToH256(parentBeaconBlockRoot),
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

func insertBlocks(ctx context.Context, exec *execmodule.EthereumExecutionModule, blocks []*types.Block) (*executionproto.InsertionResult, error) {
	rpcBlocks := make([]*executionproto.Block, len(blocks))
	for i, b := range blocks {
		rpcBlocks[i] = eth1utils.ConvertBlockToRPC(b)
	}
	return retryBusy(ctx, func() (*executionproto.InsertionResult, bool, error) {
		r, err := exec.InsertBlocks(ctx, &executionproto.InsertBlocksRequest{
			Blocks: rpcBlocks,
		})
		if err != nil {
			return nil, false, err
		}
		return r, r.Result == executionproto.ExecutionStatus_Busy, nil
	})
}

func validateChain(ctx context.Context, exec *execmodule.EthereumExecutionModule, h *types.Header) (*executionproto.ValidationReceipt, error) {
	return retryBusy(ctx, func() (*executionproto.ValidationReceipt, bool, error) {
		r, err := exec.ValidateChain(ctx, &executionproto.ValidationRequest{
			Hash:   gointerfaces.ConvertHashToH256(h.Hash()),
			Number: h.Number.Uint64(),
		})
		if err != nil {
			return nil, false, err
		}
		return r, r.ValidationStatus == executionproto.ExecutionStatus_Busy, nil
	})
}

func updateForkChoice(ctx context.Context, exec *execmodule.EthereumExecutionModule, h *types.Header) (*executionproto.ForkChoiceReceipt, error) {
	return retryBusy(ctx, func() (*executionproto.ForkChoiceReceipt, bool, error) {
		r, err := exec.UpdateForkChoice(ctx, &executionproto.ForkChoice{
			HeadBlockHash:      gointerfaces.ConvertHashToH256(h.Hash()),
			SafeBlockHash:      gointerfaces.ConvertHashToH256(common.Hash{}),
			FinalizedBlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
		})
		if err != nil {
			return nil, false, err
		}
		return r, r.Status == executionproto.ExecutionStatus_Busy, nil
	})
}

func insertValidateAndUfc1By1(ctx context.Context, exec *execmodule.EthereumExecutionModule, blocks []*types.Block) error {
	ir, err := insertBlocks(ctx, exec, blocks)
	if err != nil {
		return err
	}
	if ir.Result != executionproto.ExecutionStatus_Success {
		return fmt.Errorf("unexpected insertBlocks status: %s", ir.Result)
	}
	for _, b := range blocks {
		h := b.Header()
		vr, err := validateChain(ctx, exec, h)
		if err != nil {
			return err
		}
		if vr.ValidationStatus != executionproto.ExecutionStatus_Success {
			return fmt.Errorf("unexpected validateChain status: %s", vr.ValidationStatus)
		}
		ur, err := updateForkChoice(ctx, exec, h)
		if err != nil {
			return err
		}
		if ur.Status != executionproto.ExecutionStatus_Success {
			return fmt.Errorf("unexpected updateForkChoice status: %s", ur.Status)
		}
	}
	return nil
}

func assembleBlock(ctx context.Context, exec *execmodule.EthereumExecutionModule, req *executionproto.AssembleBlockRequest) (uint64, error) {
	return retryBusy(ctx, func() (uint64, bool, error) {
		r, err := exec.AssembleBlock(ctx, req)
		if err != nil {
			return 0, false, err
		}
		return r.Id, r.Busy, nil
	})
}

func getAssembledBlock(ctx context.Context, exe *execmodule.EthereumExecutionModule, payloadId uint64) (*types.Block, error) {
	return retryBusy(ctx, func() (*types.Block, bool, error) {
		br, busy, err := exe.GetAssembledBlockWithReceipts(payloadId)
		if err != nil {
			return nil, false, err
		}
		return br.Block, busy, nil
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
