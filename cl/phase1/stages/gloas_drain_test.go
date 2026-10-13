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

package stages

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
)

func TestDrainPendingGloasPayloadsRequeuesWithoutVerdictWhenNewPayloadIsInterrupted(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	headBlock := graph.parents[headRoot]
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	ctx, cancel := context.WithCancel(t.Context())
	engine.newPayloadFn = func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		cancel()
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	cfg.forkChoice.RequeuePendingELPayload(forkchoice.PendingELPayload{Block: headBlock, Envelope: graph.envelopes[headRoot]})
	// Through the store's own admission, which classifies the interrupted call.
	cfg.gloasPayloadValidator = cfg.forkChoice

	drainPendingGloasPayloads(ctx, cfg)

	require.Equal(t, 1, engine.newPayloadCalls)
	_, found := cfg.forkChoice.GetRecentExecutionPayloadStatusByRoot(headRoot)
	require.False(t, found, "an interrupted NewPayload is not a verdict")
	require.False(t, graph.IsPayloadUnavailable(headRoot))
	require.Len(t, cfg.forkChoice.DrainPendingELPayloads(), 1)
}
