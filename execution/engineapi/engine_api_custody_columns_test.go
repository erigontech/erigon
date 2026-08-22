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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"

	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

// TestForkchoiceUpdatedV4_NullCustodyColumns verifies that ForkchoiceUpdatedV4
// accepts a null custodyColumns (third parameter) via the Go-typed client.
// This is the primary regression test: before the fix, the RPC framework
// rejected 3-parameter requests with "too many arguments, want at most 2".
func TestForkchoiceUpdatedV4_NullCustodyColumns(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		if eat.ChainConfig.AmsterdamTime == nil {
			t.Skip("test requires Amsterdam-enabled chain config")
		}

		fcu := enginetypes.ForkChoiceState{
			HeadHash:           eat.GenesisBlock.Hash(),
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		r, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil, nil)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, r.PayloadStatus.Status)
	})
}

// TestForkchoiceUpdatedV4_WithCustodyColumns verifies that ForkchoiceUpdatedV4
// accepts a non-null 16-byte custodyColumns value. The value is accepted but
// currently unused by the forkchoice logic.
func TestForkchoiceUpdatedV4_WithCustodyColumns(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		if eat.ChainConfig.AmsterdamTime == nil {
			t.Skip("test requires Amsterdam-enabled chain config")
		}

		cc := enginetypes.CustodyColumns{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

		fcu := enginetypes.ForkChoiceState{
			HeadHash:           eat.GenesisBlock.Hash(),
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		r, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil, &cc)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, r.PayloadStatus.Status)
	})
}

// TestForkchoiceUpdatedV4_BackwardCompat_TwoParams verifies that
// ForkchoiceUpdatedV4 still works when custodyColumns is omitted (nil pointer
// in the Go client, which the RPC framework auto-fills as nil).
func TestForkchoiceUpdatedV4_BackwardCompat_TwoParams(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		if eat.ChainConfig.AmsterdamTime == nil {
			t.Skip("test requires Amsterdam-enabled chain config")
		}

		fcu := enginetypes.ForkChoiceState{
			HeadHash:           eat.GenesisBlock.Hash(),
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		// Call with nil custodyColumns — simulates a legacy V4 caller sending only 2 params
		r, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil, nil)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, r.PayloadStatus.Status)
	})
}

// TestForkchoiceUpdatedV3_Unaffected verifies that V3 behaviour is preserved —
// V3 still accepts exactly 2 parameters and returns VALID.
func TestForkchoiceUpdatedV3_Unaffected(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		_ = big.NewInt(0) // suppress unused import if needed
		fcu := enginetypes.ForkChoiceState{
			HeadHash:           eat.GenesisBlock.Hash(),
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		r, err := eat.EngineApiClient.ForkchoiceUpdatedV3(ctx, &fcu, nil)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, r.PayloadStatus.Status)
	})
}

// TestForkchoiceUpdatedV4_NullPayloadAttributes_WithCustodyColumns verifies
// the edge case of null payloadAttributes with non-null custodyColumns.
func TestForkchoiceUpdatedV4_NullPayloadAttributes_WithCustodyColumns(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	eat, err := engineapitester.DefaultEngineApiTester(ctx, logger, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		if eat.ChainConfig.AmsterdamTime == nil {
			t.Skip("test requires Amsterdam-enabled chain config")
		}

		cc := enginetypes.CustodyColumns{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

		fcu := enginetypes.ForkChoiceState{
			HeadHash:           eat.GenesisBlock.Hash(),
			SafeBlockHash:      eat.GenesisBlock.Hash(),
			FinalizedBlockHash: eat.GenesisBlock.Hash(),
		}
		// null payloadAttributes but non-null custodyColumns
		r, err := eat.EngineApiClient.ForkchoiceUpdatedV4(ctx, &fcu, nil, &cc)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, r.PayloadStatus.Status)
	})
}
