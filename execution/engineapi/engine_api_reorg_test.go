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

package engineapi_test

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

func TestEngineApiInvalidPayloadThenValidCanonicalFcuWithPayloadShouldSucceed(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
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
		b3Faulty := engineapitester.TamperMockClPayloadStateRoot(b3Canon, common.HexToHash("0xb3f"))
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
	sharedGenesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	canonicalChain := make([]*engineapitester.MockClPayload, n)
	eatCanonical := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = n
		},
	})
	eatCanonical.Run(t, func(ctx context.Context, t *testing.T, eatCanonical engineapitester.EngineApiTester) {
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
	sideChain := make([]*engineapitester.MockClPayload, n)
	eatSide := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = n
		},
	})
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eatSide engineapitester.EngineApiTester) {
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
	eatSync := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = n
		},
	})
	eatSync.Run(t, func(ctx context.Context, t *testing.T, eatSync engineapitester.EngineApiTester) {
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
	sharedGenesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	var b2Side *engineapitester.MockClPayload
	eatSide := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
	})
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// do a simple eth transfer at bn2
		txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, common.HexToAddress("0x333"), big.NewInt(1))
		require.NoError(t, err)
		b2Side, err = eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Side.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
	})
	eatCanonical := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
	})
	eatCanonical.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
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
	sharedGenesis, coinbaseKey := engineapitester.DefaultEngineApiTesterGenesis(t)
	// Generate a side chain which goes up to 1
	var sideChain *engineapitester.MockClPayload
	eatSide := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatSide.Run(t, func(ctx context.Context, t *testing.T, eatSide engineapitester.EngineApiTester) {
		txn, err := eatSide.Transactor.SubmitSimpleTransfer(eatSide.CoinbaseKey, receiver2, big.NewInt(1))
		require.NoError(t, err)
		clPayload, err := eatSide.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eatSide.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, clPayload.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		sideChain = clPayload
	})
	eatCanonical := engineapitester.InitialiseEngineApiTester(t, engineapitester.EngineApiTesterInitArgs{
		Logger:      testlog.Logger(t, logLvl),
		DataDir:     t.TempDir(),
		Genesis:     sharedGenesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = maxReorgDepth
		},
	})
	eatCanonical.Run(t, func(ctx context.Context, t *testing.T, eatCanonical engineapitester.EngineApiTester) {
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

// TestEngineApiFcuToCanonicalAncestorMovesHead verifies that an FCU pointing the
// head at a canonical ancestor of the current head actually moves the EL head
// back to that ancestor (spec: ethereum/execution-apis#770 — EL must support
// reorg to head's ancestor).
func TestEngineApiFcuToCanonicalAncestorMovesHead(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// Build canonical chain: genesis (0) + empty b1 from DefaultEngineApiTester,
		// then b2..b5 here. Head is at b5.
		receiver := common.HexToAddress("0x42")
		payloads := make([]*engineapitester.MockClPayload, 4)
		for i := range payloads {
			txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(int64(100*(i+1))))
			require.NoError(t, err)
			p, err := eat.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, p.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
			payloads[i] = p
		}
		// Sanity: head is at block 5.
		latest, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, uint64(5), latest.Number.Uint64())

		// Point FCU at block 3 (two blocks behind head), no payload attributes.
		ancestor := payloads[1]
		ancestorHash := ancestor.ExecutionPayload.BlockHash
		require.Equal(t, uint64(3), ancestor.ExecutionPayload.BlockNumber.Uint64())

		fcs := enginetypes.ForkChoiceState{
			HeadHash:           ancestorHash,
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		resp, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcs, nil)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, resp.PayloadStatus.Status)
		require.NotNil(t, resp.PayloadStatus.LatestValidHash)
		require.Equal(t, ancestorHash, *resp.PayloadStatus.LatestValidHash)
		require.Nil(t, resp.PayloadId)

		// EL head must now be the ancestor.
		latest, err = eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, uint64(3), latest.Number.Uint64())
		require.Equal(t, ancestorHash, latest.Hash)
	})
}

// TestEngineApiFcuToCanonicalAncestorWithPayloadAttributes verifies that an FCU
// with payload attributes whose head is a canonical ancestor triggers a reorg
// to the ancestor and begins building a payload on top of it (spec:
// ethereum/execution-apis#770).
func TestEngineApiFcuToCanonicalAncestorWithPayloadAttributes(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		receiver := common.HexToAddress("0x42")
		payloads := make([]*engineapitester.MockClPayload, 4)
		for i := range payloads {
			txn, err := eat.Transactor.SubmitSimpleTransfer(eat.CoinbaseKey, receiver, big.NewInt(int64(100*(i+1))))
			require.NoError(t, err)
			p, err := eat.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, p.ExecutionPayload, txn.Hash())
			require.NoError(t, err)
			payloads[i] = p
		}
		// Head is at block 5. Target ancestor: block 3.
		ancestor := payloads[1]
		ancestorHash := ancestor.ExecutionPayload.BlockHash
		require.Equal(t, uint64(3), ancestor.ExecutionPayload.BlockNumber.Uint64())

		// Build payload attributes for a sibling of block 4, on top of block 3.
		newTimestamp := uint64(ancestor.ExecutionPayload.Timestamp) + 1
		parentBeaconBlockRoot := common.HexToHash("0xfeedface")
		slotNum := eat.MockCl.State().NextSlotNumber()
		attrs := &enginetypes.PayloadAttributes{
			Timestamp:             hexutil.Uint64(newTimestamp),
			PrevRandao:            common.HexToHash("0xdeadbeef"),
			SuggestedFeeRecipient: eat.GenesisBlock.Coinbase(),
			Withdrawals:           []*types.Withdrawal{},
			ParentBeaconBlockRoot: &parentBeaconBlockRoot,
			SlotNumber:            (*hexutil.Uint64)(&slotNum),
		}
		fcs := enginetypes.ForkChoiceState{
			HeadHash:           ancestorHash,
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		resp, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcs, attrs)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, resp.PayloadStatus.Status)
		require.NotNil(t, resp.PayloadStatus.LatestValidHash)
		require.Equal(t, ancestorHash, *resp.PayloadStatus.LatestValidHash)
		require.NotNil(t, resp.PayloadId, "payloadId expected when building on ancestor")

		// Retrieve the newly built payload and verify it is built on top of the ancestor.
		newPayload, err := eat.EngineApiClient.GetPayloadV6(ctx, *resp.PayloadId)
		require.NoError(t, err)
		require.Equal(t, ancestorHash, newPayload.ExecutionPayload.ParentHash)
		require.Equal(t, uint64(4), uint64(newPayload.ExecutionPayload.BlockNumber))
		require.Equal(t, newTimestamp, uint64(newPayload.ExecutionPayload.Timestamp))

		// EL head should be the ancestor until the newly built block is promoted.
		latest, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, uint64(3), latest.Number.Uint64())
		require.Equal(t, ancestorHash, latest.Hash)
	})
}

// TestEngineApiFcuToDistantCanonicalAncestorSkipsReorg verifies the MAY-skip branch
// of ethereum/execution-apis#770: when the requested head is a canonical ancestor
// more than MaxAncestorReorgDepth (32) blocks behind the current head, the EL may
// skip the forkchoice update and must return {VALID, latestValidHash: head, payloadId: null}
// without moving the EL head.
func TestEngineApiFcuToDistantCanonicalAncestorSkipsReorg(t *testing.T) {
	eat := engineapitester.DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		// DefaultEngineApiTester already built block 1 (empty). Build 35 more so head
		// is at block 36 and the depth to block 3 is 33 (> 32).
		const extraBlocks = 35
		payloads := make([]*engineapitester.MockClPayload, extraBlocks)
		for i := range payloads {
			p, err := eat.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
			payloads[i] = p
		}
		latest, err := eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		currentHeadNumber := uint64(extraBlocks) + 1
		require.Equal(t, currentHeadNumber, latest.Number.Uint64())
		currentHeadHash := latest.Hash

		// Target: block 3 (33 blocks behind head at block 36).
		ancestor := payloads[1]
		ancestorHash := ancestor.ExecutionPayload.BlockHash
		require.Equal(t, uint64(3), ancestor.ExecutionPayload.BlockNumber.Uint64())
		require.Greater(t, currentHeadNumber-3, uint64(32), "need depth > MaxAncestorReorgDepth")

		// FCU to the distant ancestor must be skipped: response VALID, payloadId nil, head unmoved.
		fcs := enginetypes.ForkChoiceState{
			HeadHash:           ancestorHash,
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		resp, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcs, nil)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, resp.PayloadStatus.Status)
		require.NotNil(t, resp.PayloadStatus.LatestValidHash)
		require.Equal(t, ancestorHash, *resp.PayloadStatus.LatestValidHash)
		require.Nil(t, resp.PayloadId)

		latest, err = eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, currentHeadNumber, latest.Number.Uint64())
		require.Equal(t, currentHeadHash, latest.Hash)

		// Even when payload attributes are provided, the skip path must return payloadId=nil
		// and must not build a payload atop the distant ancestor.
		newTimestamp := uint64(ancestor.ExecutionPayload.Timestamp) + 1
		parentBeaconBlockRoot := common.HexToHash("0xcafeface")
		slotNum := eat.MockCl.State().NextSlotNumber()
		attrs := &enginetypes.PayloadAttributes{
			Timestamp:             hexutil.Uint64(newTimestamp),
			PrevRandao:            common.HexToHash("0xdeadbeef"),
			SuggestedFeeRecipient: eat.GenesisBlock.Coinbase(),
			Withdrawals:           []*types.Withdrawal{},
			ParentBeaconBlockRoot: &parentBeaconBlockRoot,
			SlotNumber:            (*hexutil.Uint64)(&slotNum),
		}
		resp, err = eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcs, attrs)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, resp.PayloadStatus.Status)
		require.Nil(t, resp.PayloadId, "payloadId must be nil when skipping distant ancestor")

		latest, err = eat.RpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
		require.NoError(t, err)
		require.Equal(t, currentHeadNumber, latest.Number.Uint64())
		require.Equal(t, currentHeadHash, latest.Hash)
	})
}
