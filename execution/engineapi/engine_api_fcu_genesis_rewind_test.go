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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

// A forkchoiceUpdated that moves the head back to a canonical ancestor (here
// genesis) must be honored: the enginex consume simulator resets a reused
// client between tests with FCU(head=genesis), so a client whose head was
// advanced by the previous test needs this head regression to succeed.
func TestEngineApiForkchoiceToGenesisRewindsHead(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, eat.Close())
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		genesisHash := eat.GenesisBlock.Hash()
		for range 3 {
			_, err := eat.MockCl.BuildCanonicalBlock(ctx)
			require.NoError(t, err)
		}
		headBefore, err := eat.RpcApiClient.BlockNumber()
		require.NoError(t, err)
		require.Greater(t, headBefore, uint64(0), "head should have advanced")

		fcu := enginetypes.ForkChoiceState{
			HeadHash:           genesisHash,
			SafeBlockHash:      genesisHash,
			FinalizedBlockHash: genesisHash,
		}
		var r *enginetypes.ForkChoiceUpdatedResponse
		if eat.ChainConfig.AmsterdamTime != nil {
			r, err = eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil)
		} else {
			r, err = eat.EngineApiClient.ForkchoiceUpdatedV3(ctx, &fcu, nil)
		}
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, r.PayloadStatus.Status,
			"FCU(head=genesis) should rewind the head and return VALID")

		headAfter, err := eat.RpcApiClient.BlockNumber()
		require.NoError(t, err)
		require.Equal(t, uint64(0), headAfter, "head should have rewound to genesis")
	})
}
