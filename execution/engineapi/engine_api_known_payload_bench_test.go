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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

// BenchmarkNewPayloadKnownSideBlock re-sends a validated payload while a sibling block is the head.
func BenchmarkNewPayloadKnownSideBlock(b *testing.B) {
	const numTx = 1000
	ctx := b.Context()
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(b, err)
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     b.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.TxPool.AccountSlots = 2 * numTx
		},
	})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, eat.Close()) })

	sender := crypto.PubkeyToAddress(coinbaseKey.PublicKey)
	nonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.PendingBlock)
	require.NoError(b, err)
	signer := types.LatestSignerForChainID(eat.ChainId())
	for i := range uint64(numTx) {
		txn := types.MustSignNewTx(coinbaseKey, *signer, &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce.Uint64() + i, GasLimit: 21_000, To: &sender},
			ChainID:  *eat.ChainId(),
			TipCap:   *uint256.NewInt(1_000_000_000),
			FeeCap:   *uint256.NewInt(100_000_000_000),
		})
		_, err := eat.RpcApiClient.SendTransaction(txn)
		require.NoError(b, err)
	}

	var known *engineapitester.MockClPayload
	require.Eventually(b, func() bool {
		known, err = eat.MockCl.BuildNewPayload(ctx)
		require.NoError(b, err)
		return len(known.ExecutionPayload.Transactions) == numTx
	}, time.Minute, 100*time.Millisecond)
	status, err := eat.MockCl.InsertNewPayload(ctx, known)
	require.NoError(b, err)
	require.Equal(b, enginetypes.ValidStatus, status.Status)
	head, err := eat.MockCl.BuildCanonicalBlock(ctx, engineapitester.WithTimestamp(known.ExecutionPayload.Timestamp.Uint64()+1))
	require.NoError(b, err)
	require.Len(b, head.ExecutionPayload.Transactions, numTx)

	for b.Loop() {
		status, err := eat.MockCl.InsertNewPayload(ctx, known)
		require.NoError(b, err)
		require.Equal(b, enginetypes.ValidStatus, status.Status)
	}
}
