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
	eat := newKnownPayloadTester(b, numTx)
	known := buildPayloadWithTransfers(b, eat, numTx)
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

// BenchmarkNewPayloadResentHeadThenFcu re-sends a validated head payload before its forkchoice update.
func BenchmarkNewPayloadResentHeadThenFcu(b *testing.B) {
	const numTx = 200
	ctx := b.Context()
	eat := newKnownPayloadTester(b, numTx)
	for b.Loop() {
		b.StopTimer()
		payload := buildPayloadWithTransfers(b, eat, numTx)
		status, err := eat.MockCl.InsertNewPayload(ctx, payload)
		require.NoError(b, err)
		require.Equal(b, enginetypes.ValidStatus, status.Status)
		b.StartTimer()
		status, err = eat.MockCl.InsertNewPayload(ctx, payload)
		require.NoError(b, err)
		require.Equal(b, enginetypes.ValidStatus, status.Status)
		require.NoError(b, eat.MockCl.UpdateForkChoice(ctx, payload))
	}
}

func newKnownPayloadTester(b *testing.B, numTx int) engineapitester.EngineApiTester {
	logger := log.New()
	logger.SetHandler(log.DiscardHandler())
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(b, err)
	eat, err := engineapitester.InitialiseEngineApiTester(b.Context(), engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     b.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.TxPool.AccountSlots = 2 * uint64(numTx)
		},
	})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, eat.Close()) })
	return eat
}

func sendTransfers(b *testing.B, eat engineapitester.EngineApiTester, numTx int) {
	sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
	nonce, err := eat.RpcApiClient.GetTransactionCount(sender, rpc.PendingBlock)
	require.NoError(b, err)
	signer := types.LatestSignerForChainID(eat.ChainId())
	for i := range uint64(numTx) {
		txn := types.MustSignNewTx(eat.CoinbaseKey, *signer, &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce.Uint64() + i, GasLimit: 21_000, To: &sender},
			ChainID:  *eat.ChainId(),
			TipCap:   *uint256.NewInt(1_000_000_000),
			FeeCap:   *uint256.NewInt(100_000_000_000),
		})
		_, err := eat.RpcApiClient.SendTransaction(txn)
		require.NoError(b, err)
	}
}

func buildPayloadWithTransfers(b *testing.B, eat engineapitester.EngineApiTester, numTx int) *engineapitester.MockClPayload {
	sendTransfers(b, eat, numTx)
	var payload *engineapitester.MockClPayload
	require.Eventually(b, func() bool {
		var err error
		payload, err = eat.MockCl.BuildNewPayload(b.Context())
		require.NoError(b, err)
		return len(payload.ExecutionPayload.Transactions) == numTx
	}, time.Minute, 100*time.Millisecond)
	return payload
}
