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

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
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
		t.Log(status.ValidationError.Error().Error())
		require.True(t, strings.Contains(status.ValidationError.Error().Error(), "invalid block hash"))
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
	sharedGenesis, coinbaseKey := blockgen.DefaultEngineApiTesterGenesis(t)
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
	eatSync.Run(t, func(ctx context.Context, t *testing.T, eatSync EngineApiTester) {
		for _, payload := range canonicalChain {
			_, err := eatSync.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
		}
		err := eatSync.MockCl.UpdateForkChoice(ctx, canonicalChain[len(canonicalChain)-1])
		require.NoError(t, err)
		_, err = eatSync.MockCl.InsertNewPayload(ctx, sideChain[len(sideChain)-1])
		require.NoError(t, err)
	})
}

func TestEthGetLogsDoNotGetAffectedAfterNewPayloadOnSideChain(t *testing.T) {
	logLvl := log.LvlDebug
	sharedGenesis, coinbaseKey := blockgen.DefaultEngineApiTesterGenesis(t)
	var b2Side *MockClPayload
	eatSide := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
	})
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
		// do a simple eth transfer at bn2
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, common.HexToAddress("0x333"), big.NewInt(1))
		require.NoError(t, err)
		b2Side, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Side.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
	})
	eatCanonical := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
	})
	eatCanonical.Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
		// deploy a smart contract at bn2
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit
		_, txn, poly, err := contracts.DeployPoly(transactOpts, eat.ContractBackend)
		require.NoError(t, err)
		b2Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		// interact with the smart contract at bn3 to produce a log
		txn, err = poly.Deploy(transactOpts, big.NewInt(1))
		require.NoError(t, err)
		b3Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b3Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		// check the log is present by querying rpc
		latestBlock, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, uint64(3), latestBlock.Number.Uint64())
		logs, err := eat.RpcApiClient.FilterLogs(ctx, ethereum.FilterQuery{BlockHash: &latestBlock.Hash})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		require.Equal(t, uint64(3), logs[0].BlockNumber)
		// now insert a new payload on the side chain and check the log is still present
		_, err = eat.MockCl.InsertNewPayload(ctx, b2Side)
		require.NoError(t, err)
		logs, err = eat.RpcApiClient.FilterLogs(ctx, ethereum.FilterQuery{BlockHash: &latestBlock.Hash})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		require.Equal(t, uint64(3), logs[0].BlockNumber)
	})
}

func TestNewPayloadShouldReturnValidWhenSideChainGoingBackIsLtMaxReorgDepth(t *testing.T) {
	// we had an issue where some benchmark tests were doing more than a 32-block reorg backwards
	// while our MAX_REORG_DEPTH was 96 blocks, however, NewPayload returned ACCEPTED instead of VALID
	// and caused issues with benchmarkoor
	// this test captures that by calling NewPayload for a side fork back to block 1 from canonical fork at block 34
	logLvl := log.LvlDebug
	canonicalChainLen := 34 // a few blocks > 32
	maxReorgDepth := uint64(96)
	receiver1 := common.HexToAddress("0x111")
	receiver2 := common.HexToAddress("0x222")
	sharedGenesis, coinbaseKey := blockgen.DefaultEngineApiTesterGenesis(t)
	// Generate a side chain which goes up to 1
	var sideChain *MockClPayload
	eatSide := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eatSide EngineApiTester) {
		txn, err := eatSide.Transactor.SubmitSimpleTransfer(eatSide.CoinbaseKey, receiver2, big.NewInt(1))
		require.NoError(t, err)
		clPayload, err := eatSide.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eatSide.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, clPayload.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		sideChain = clPayload
	})
	eatCanonical := InitialiseEngineApiTester(t, EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatCanonical.Run(t, func(ctx context.Context, t *testing.T, eatCanonical EngineApiTester) {
		// build the canonical chain up to canonicalChainLen
		for i := 0; i < canonicalChainLen; i++ {
			txn, err := eatCanonical.Transactor.SubmitSimpleTransfer(eatCanonical.CoinbaseKey, receiver1, big.NewInt(1))
			require.NoError(t, err)
			clPayload, err := eatCanonical.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			err = eatCanonical.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, clPayload.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
		}
		// now do a new payload on the side chain at block 1
		ps, err := eatCanonical.MockCl.InsertNewPayload(ctx, sideChain)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, ps.Status)
	})
}
