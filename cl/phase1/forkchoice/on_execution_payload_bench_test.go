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

package forkchoice

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
)

func BenchmarkApplyEnvelopeValidatedPath(b *testing.B) {
	for _, tc := range []struct {
		transactions int
		balBytes     int
	}{
		{transactions: 300, balBytes: 100_000},
		{transactions: 1500, balBytes: 400_000},
	} {
		b.Run(fmt.Sprintf("txs=%d/bal=%dKB", tc.transactions, tc.balBytes/1000), func(b *testing.B) {
			cfg, blockState, block, envelope := benchmarkEnvelopeFixture(b, tc.transactions, tc.balBytes)
			root := envelope.Message.BeaconBlockRoot
			store, graph := newEnvelopeApplicationTestStore(b, cfg, blockState, block)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				graph.mu.Lock()
				graph.envelope = nil
				graph.mu.Unlock()
				require.False(b, graph.HasEnvelope(root))
				iterationEnvelope := decodeBenchmarkEnvelope(b, cfg, envelope)
				b.StartTimer()
				require.NoError(b, store.OnExecutionPayload(b.Context(), iterationEnvelope, false, true))
				b.StopTimer()
				require.True(b, graph.HasEnvelope(root))
				b.StartTimer()
			}
		})
	}
}

func decodeBenchmarkEnvelope(
	b *testing.B,
	cfg *clparams.BeaconChainConfig,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
) *cltypes.SignedExecutionPayloadEnvelope {
	b.Helper()
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(b, err)
	decoded := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	require.NoError(b, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	return decoded
}

func benchmarkEnvelopeFixture(
	b *testing.B,
	transactionCount int,
	balSize int,
) (*clparams.BeaconChainConfig, *state2.CachingBeaconState, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) {
	b.Helper()
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(b)
	transactions := make([][]byte, transactionCount)
	for i := range transactions {
		transactions[i] = make([]byte, 256)
		_, err := rand.Read(transactions[i])
		require.NoError(b, err)
	}
	envelope.Message.Payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(transactions)
	bal := make([]byte, balSize)
	_, err := rand.Read(bal)
	require.NoError(b, err)
	require.NoError(b, envelope.Message.Payload.BlockAccessList.SetBytes(bal))

	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, envelope.Message.ExecutionRequests))
	payloadHash, err := envelope.Message.Payload.ComputeBlockHash(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(b, err)
	envelope.Message.Payload.BlockHash = payloadHash
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BlockHash = payloadHash
	blockState.SetLatestExecutionPayloadBid(bid)
	rebindAdmissionBlockRoot(b, blockState, block, envelope)
	resignAdmissionEnvelope(b, cfg, blockState, envelope)
	return cfg, blockState, block, envelope
}
