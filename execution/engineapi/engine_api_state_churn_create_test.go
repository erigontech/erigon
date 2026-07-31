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

package engineapi_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestEngineApiReorgAcrossContractCreationAtSameAddress reorgs across the
// block that CREATEs the churn contract: both forks deploy from the same
// sender and nonce, so the contract lands at the same address on both, but in
// different blocks with different subsequent churn. Re-executing the winning
// fork's deployment only succeeds if the unwind removed the losing fork's
// account completely — leftover code or nonce at the address triggers the
// EIP-684 create-collision (the consume-all-gas failure behind the phantom
// CREATE2 incidents), and leftover storage trips the churn invariant.
func TestEngineApiReorgAcrossContractCreationAtSameAddress(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	sharedGenesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	tweak := func(config *ethconfig.Config) { config.MaxReorgDepth = stateChurnReorgDepthBudget }

	canonPayloads, canonAddr, canonRef := buildRecordedChurnChain(ctx, t, logger, sharedGenesis, coinbaseKey, tweak, 5, func(k int) int64 { return int64(k) })
	// The side fork diverges at the deploy block itself: same deploy initcode
	// and nonce (hence the same contract address), different gas price (hence
	// a different transaction and block hash).
	sidePayloads, sideAddr, sideRef := buildRecordedChurnChain(ctx, t, logger, sharedGenesis, coinbaseKey, tweak, 6,
		func(k int) int64 { return int64(k) + 1_000_000 },
		func(opts *bind.TransactOpts) { opts.GasPrice = big.NewInt(2_000_000_000) },
	)

	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger: logger, DataDir: t.TempDir(), Genesis: sharedGenesis, CoinbaseKey: coinbaseKey, EthConfigTweaker: tweak,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		require.Equal(t, canonAddr, sideAddr, "same sender+nonce must create at the same address on both forks")
		require.NotEqual(t, canonPayloads[0].ExecutionPayload.BlockHash, sidePayloads[0].ExecutionPayload.BlockHash,
			"the deploy blocks must genuinely differ so the reorg crosses the creation")
		canonTip := canonPayloads[len(canonPayloads)-1]
		sideTip := sidePayloads[len(sidePayloads)-1]
		churn, err := contracts.NewStateChurn(canonAddr, eat.ContractBackend)
		require.NoError(t, err)

		for _, payload := range canonPayloads {
			status, err := eat.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
			require.Equal(t, enginetypes.ValidStatus, status.Status)
		}
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, canonTip))
		assertChurnState(ctx, t, eat, churn, canonTip, canonRef)

		// Reorg onto the side fork: unwinds through the canonical deployment
		// and re-creates the contract at the same address.
		for _, payload := range sidePayloads {
			status, err := eat.MockCl.InsertNewPayload(ctx, payload)
			require.NoError(t, err)
			require.Equalf(t, enginetypes.ValidStatus, status.Status, "insert of side payload at height %d", payload.ExecutionPayload.BlockNumber.Uint64())
		}
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, sideTip))
		assertChurnState(ctx, t, eat, churn, sideTip, sideRef)

		// And back again across the creation in the other direction.
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, canonTip))
		assertChurnState(ctx, t, eat, churn, canonTip, canonRef)

		// The surviving contract must remain fully usable.
		churnAndAssert(ctx, t, eat, churn, 3, func(k int) int64 { return int64(7_000 + k) })
	})
}
