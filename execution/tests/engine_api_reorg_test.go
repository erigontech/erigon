// Copyright 2025 The Erigon Authors
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

package executiontests

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
)

func TestEngineApiInvalidPayloadThenValidCanonicalFcuWithPayloadShouldSucceed(t *testing.T) {
	eat := DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
		// deploy changer at b2
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit
		_, txn, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
		require.NoError(t, err)
		b2Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		// change changer at b3
		txn, err = changer.Change(transactOpts)
		require.NoError(t, err)
		b3Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b3Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		// create an invalid fork at b3
		b3Faulty := TamperMockClPayloadStateRoot(b3Canon, common.HexToHash("0xb3f"))
		status, err := eat.MockCl.InsertNewPayload(ctx, b3Faulty)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.True(t, strings.Contains(status.ValidationError.Error().Error(), "wrong trie root"))
		// build b4 on the canonical chain
		txn, err = changer.Change(transactOpts)
		require.NoError(t, err)
		b4Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b4Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
	})
}

func TestEngineApiExecBlockBatchWithLenLtMaxReorgDepthAtTipThenUnwindShouldSucceed(t *testing.T) {
	// Scenario:
	//   - we were following the tip efficiently and exec-ing 1 block at a time
	//   - the CL went offline for a some time
	//   - when it came back online we executed
	//   - fcu1 with 357 blocks forward, no unwinding (it connected to the last fcu before CL went offline)
	//   - fcu2 with 64 blocks forward, no unwinding (it connected to fcu1)
	//   - newPayload for a side chain at height fcu2+1 that requires an unwind to height fcu2-1
	//   - we got stuck with error domains.GetDiffset(fcu2.BlockNum, fcu2.BlockHash) not found
	//
	// In this test we simplify this to just exec-ing a batch of N blocks <= maxReorgDepth and then
	// doing a new payload with a side chain which requires 1 block unwind.
	//
	// Generate a canonical chain of N blocks
	n := uint64(64)
	logLvl := log.LvlDebug
	receiver1 := common.HexToAddress("0x111")
	receiver2 := common.HexToAddress("0x222")
	sharedGenesis, coinbaseKey := DefaultEngineApiTesterGenesis(t)
	canonicalChain := make([]*MockClPayload, n)
	eatCanonical := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = n
		},
	})
	eatCanonical.Run(t, func(ctx context.Context, t *testing.T, eatCanonical EngineApiTester) {
		for i := range canonicalChain {
			txn, err := eatCanonical.Transactor.SubmitSimpleTransfer(eatCanonical.CoinbaseKey, receiver1, big.NewInt(1))
			require.NoError(t, err)
			clPayload, err := eatCanonical.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			err = eatCanonical.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, clPayload.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
			canonicalChain[i] = clPayload
		}
	})
	// Generate a side chain which goes up to N and executes the same txns until N-1 but at N executes different txns
	sideChain := make([]*MockClPayload, n)
	eatSide := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = n
		},
	})
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eatSide EngineApiTester) {
		forkPoint := n - 1
		for i := range sideChain[:forkPoint] {
			txn, err := eatSide.Transactor.SubmitSimpleTransfer(eatSide.CoinbaseKey, receiver1, big.NewInt(1))
			require.NoError(t, err)
			clPayload, err := eatSide.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			err = eatSide.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, clPayload.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
			sideChain[i] = clPayload
		}
		for i := forkPoint; i < uint64(len(sideChain)); i++ {
			txn, err := eatSide.Transactor.SubmitSimpleTransfer(eatSide.CoinbaseKey, receiver2, big.NewInt(1))
			require.NoError(t, err)
			clPayload, err := eatSide.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			err = eatSide.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, clPayload.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
			sideChain[i] = clPayload
		}
	})
	// Sync another EL all the way up to the canonical tip, then give it the side chain tip as a new payload
	eatSync := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = n
		},
	})
	// getCurrentBlockHash returns the hash of the last canonical block, i.e. what you get from LastHeader table
	getCurrentBlockHash := func(ctx context.Context) (currentBlockHash common.Hash, err error) {
		headerResponse, err := eatSync.EthereumBackend.ExecutionModule().CurrentHeader(ctx, &emptypb.Empty{})
		currentBlockHash = gointerfaces.ConvertH256ToHash(headerResponse.Header.BlockHash)
		return
	}
	// getForkChoiceHeadBlockHash returns the block hash for the last fork choice i.e. what you get from LastForkChoiceHead table
	getForkChoiceHeadBlockHash := func(ctx context.Context) (headBlockHash common.Hash, err error) {
		forkChoice, err := eatSync.EthereumBackend.ExecutionModule().GetForkChoice(ctx, &emptypb.Empty{})
		headBlockHash = gointerfaces.ConvertH256ToHash(forkChoice.HeadBlockHash)
		return
	}
	// getMaxTxNum returns the maximum TxNum associated with the given block number
	getMaxTxNum := func(ctx context.Context, blockNum uint64) (maxTxNum uint64, err error) {
		chainDB := eatSync.EthereumBackend.ChainDB()
		roTx, err := chainDB.BeginRo(ctx)
		if err != nil {
			return 0, err
		}
		defer roTx.Rollback()
		maxTxNum, err = rawdbv3.TxNums.Max(roTx, blockNum)
		return
	}
	eatSync.Run(t, func(ctx context.Context, t *testing.T, eatSync EngineApiTester) {
		// After initialization the chain comprises the genesys block (0) + an empty block (1) => the head is at block 1
		block1Hash := eatSync.MockCl.State().ParentElBlock
		headBlockHash, err := getForkChoiceHeadBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, block1Hash, headBlockHash)
		currentBlockHash, err := getCurrentBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, block1Hash, currentBlockHash)
		maxTxNum, err := getMaxTxNum(ctx, n+1)
		require.NoError(t, err)
		require.Equal(t, uint64(3), maxTxNum)

		// Insert the canonical chain without any ForkChoiceUpdated yet => the head must still be at block 1
		for _, payload := range canonicalChain {
			_, err := eatSync.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
		}
		headBlockHash, err = getForkChoiceHeadBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, block1Hash, headBlockHash)
		maxTxNum, err = getMaxTxNum(ctx, n+1)
		require.NoError(t, err)
		require.Equal(t, uint64(3), maxTxNum)

		// Set fork choice to the last canonical block => the head must be updated accordingly
		err = eatSync.MockCl.UpdateForkChoice(ctx, canonicalChain[len(canonicalChain)-1])
		require.NoError(t, err)
		headBlockHash, err = getForkChoiceHeadBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, canonicalChain[len(canonicalChain)-1].ExecutionPayload.BlockHash, headBlockHash)
		currentBlockHash, err = getCurrentBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, canonicalChain[len(canonicalChain)-1].ExecutionPayload.BlockHash, currentBlockHash)
		maxTxNum, err = getMaxTxNum(ctx, n+1)
		require.NoError(t, err)
		require.Equal(t, uint64(195), maxTxNum)

		// Insert new payload on a side fork => this triggers an unwind which should not be committed, so same head
		_, err = eatSync.MockCl.InsertNewPayload(ctx, sideChain[len(sideChain)-1])
		require.NoError(t, err)
		headBlockHash, err = getForkChoiceHeadBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, canonicalChain[len(canonicalChain)-1].ExecutionPayload.BlockHash, headBlockHash)
		currentBlockHash, err = getCurrentBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, canonicalChain[len(canonicalChain)-1].ExecutionPayload.BlockHash, currentBlockHash)
		maxTxNum, err = getMaxTxNum(ctx, n+1)
		require.NoError(t, err)
		require.Equal(t, uint64(195), maxTxNum)

		// Set fork choice to the last block of the side fork => the head must be updated accordingly
		err = eatSync.MockCl.UpdateForkChoice(ctx, sideChain[len(sideChain)-1])
		require.NoError(t, err)
		headBlockHash, err = getForkChoiceHeadBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, sideChain[len(sideChain)-1].ExecutionPayload.BlockHash, headBlockHash)
		currentBlockHash, err = getCurrentBlockHash(ctx)
		require.NoError(t, err)
		require.Equal(t, sideChain[len(sideChain)-1].ExecutionPayload.BlockHash, currentBlockHash)
		maxTxNum, err = getMaxTxNum(ctx, n+1)
		require.NoError(t, err)
		require.Equal(t, uint64(195), maxTxNum)
	})
}
