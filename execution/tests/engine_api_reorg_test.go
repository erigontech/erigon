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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/node/ethconfig"
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
