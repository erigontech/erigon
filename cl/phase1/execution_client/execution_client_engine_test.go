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

package execution_client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

func TestMarkRemoteRequestAbandoned(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	tests := []struct {
		name       string
		ctx        context.Context
		err        error
		abandoned  bool
		contextErr error
	}{
		{
			name:       "matching request cancellation",
			ctx:        canceledCtx,
			err:        fmt.Errorf("rpc call: %w", context.Canceled),
			abandoned:  true,
			contextErr: context.Canceled,
		},
		{
			name:      "unrelated remote failure after cancellation",
			ctx:       canceledCtx,
			err:       errors.New("remote builder failed"),
			abandoned: false,
		},
		{
			name:      "unrelated context error while request is active",
			ctx:       t.Context(),
			err:       fmt.Errorf("remote builder failed: %w", context.Canceled),
			abandoned: false,
		},
		{
			name:      "remote engine deadline string while request is active",
			ctx:       t.Context(),
			err:       errors.New(remoteForkChoiceTimeoutMessage),
			abandoned: false,
		},
		{
			name:      "remote engine deadline string racing caller cancellation",
			ctx:       canceledCtx,
			err:       errors.New(remoteForkChoiceTimeoutMessage),
			abandoned: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := markRemoteRequestAbandoned(test.ctx, test.err)
			require.Equal(t, test.abandoned, errors.Is(err, execmodule.ErrRequestAbandoned))
			require.ErrorIs(t, err, test.err)
			if test.contextErr != nil {
				require.ErrorIs(t, err, test.contextErr)
			}
		})
	}
}

type timedOutForkChoiceEngine struct {
	engineapi.EngineAPI
}

func (timedOutForkChoiceEngine) ForkchoiceUpdatedV1(context.Context, *engine_types.ForkChoiceState, *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return nil, errors.New(remoteForkChoiceTimeoutMessage)
}

func TestRemoteForkChoiceUpdateToleratesEngineTimeout(t *testing.T) {
	client := &ExecutionClientEngine{engine: timedOutForkChoiceEngine{}}

	payloadID, err := client.ForkChoiceUpdate(t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.BellatrixVersion)

	require.NoError(t, err)
	require.Nil(t, payloadID)
}

type canceledNewPayloadEngine struct {
	engineapi.EngineAPI
}

func (canceledNewPayloadEngine) NewPayloadV1(ctx context.Context, _ *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return nil, fmt.Errorf("remote NewPayload: %w", ctx.Err())
}

func TestRemoteNewPayloadMarksRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	payload := cltypes.NewEth1Block(clparams.BellatrixVersion, &clparams.MainnetBeaconConfig)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)

	client := &ExecutionClientEngine{engine: canceledNewPayloadEngine{}}
	_, err := client.NewPayload(ctx, payload, nil, nil, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
}

func TestGetAssembledBlockRejectsMissingBlobsBundle(t *testing.T) {
	client := &ExecutionClientEngine{beaconCfg: &clparams.MainnetBeaconConfig}
	resp := &engine_types.GetPayloadResponse{ExecutionPayload: &engine_types.ExecutionPayload{}}

	payload, blobs, requests, value, err := client.getAssembledBlockFromResponse(resp, clparams.DenebVersion)

	require.Nil(t, payload)
	require.Nil(t, blobs)
	require.Nil(t, requests)
	require.Nil(t, value)
	require.EqualError(t, err, "GetPayload returned nil blobs bundle")
}

type beaconCfgEngineStub struct {
	engineapi.EngineAPI
	cfg *clparams.BeaconChainConfig
}

func (s *beaconCfgEngineStub) SetBeaconChainConfig(cfg *clparams.BeaconChainConfig) {
	s.cfg = cfg
}

func TestNewExecutionClientEngineLocalPropagatesBeaconConfig(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	engine := &beaconCfgEngineStub{}

	client, err := NewExecutionClientEngineLocal(engine, chainreader.ChainReaderWriterEth1{}, nil, &cfg)
	require.NoError(t, err)

	require.Same(t, &cfg, client.beaconCfg)
	require.Same(t, &cfg, engine.cfg)
}

func TestExecutionPayloadFromSSZBlock_BlockAccessListGloasOnly(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig

	t.Run("pre-Gloas payload omits blockAccessList", func(t *testing.T) {
		block := cltypes.NewEth1Block(clparams.ElectraVersion, &beaconCfg)
		block.Extra = solid.NewExtraData()
		block.Transactions = &solid.TransactionsSSZ{}
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)

		ep := engine_types.ExecutionPayloadFromSSZBlock(block, block.Version())

		raw, err := json.Marshal(ep)
		require.NoError(t, err)

		var m map[string]any
		require.NoError(t, json.Unmarshal(raw, &m))
		_, ok := m["blockAccessList"]
		require.False(t, ok, "blockAccessList must be absent from JSON for pre-Gloas blocks")
	})

	gloasTests := []struct {
		name    string
		block   *cltypes.Eth1Block
		wantHex string
	}{
		{
			name:    "Gloas block with nil ByteListSSZ",
			block:   gloas(&beaconCfg, nil),
			wantHex: "0x",
		},
		{
			name:    "Gloas block with empty ByteListSSZ data",
			block:   gloas(&beaconCfg, []byte{}),
			wantHex: "0x",
		},
		{
			name:    "Gloas block with RLP empty list",
			block:   gloas(&beaconCfg, []byte{0xc0}),
			wantHex: "0xc0",
		},
		{
			name:    "Gloas block with non-empty BAL",
			block:   gloas(&beaconCfg, []byte{0xc1, 0x80}),
			wantHex: "0xc180",
		},
	}

	for _, tt := range gloasTests {
		t.Run(tt.name, func(t *testing.T) {
			ep := engine_types.ExecutionPayloadFromSSZBlock(tt.block, tt.block.Version())

			raw, err := json.Marshal(ep)
			require.NoError(t, err)

			var m map[string]any
			require.NoError(t, json.Unmarshal(raw, &m))

			bal, ok := m["blockAccessList"]
			require.True(t, ok, "blockAccessList must be present in JSON for Gloas+ blocks")
			require.Equal(t, tt.wantHex, bal)
		})
	}
}

func gloas(cfg *clparams.BeaconChainConfig, balData []byte) *cltypes.Eth1Block {
	block := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	block.Extra = solid.NewExtraData()
	block.Transactions = &solid.TransactionsSSZ{}
	block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	if balData != nil {
		bal := solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
		_ = bal.SetBytes(balData)
		block.BlockAccessList = bal
	} else {
		block.BlockAccessList = nil
	}
	return block
}
