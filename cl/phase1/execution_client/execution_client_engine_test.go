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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

type fcuEngineStub struct {
	engineapi.EngineAPI
	response *engine_types.ForkChoiceUpdatedResponse
	err      error
}

func (s *fcuEngineStub) ForkchoiceUpdatedV3(context.Context, *engine_types.ForkChoiceState, *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return s.response, s.err
}

type getPayloadEngineStub struct {
	engineapi.EngineAPI
	response *engine_types.GetPayloadResponse
	called   string
}

func (s *getPayloadEngineStub) GetPayloadV3(context.Context, hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	s.called = "V3"
	return s.response, nil
}

func (s *getPayloadEngineStub) GetPayloadV4(context.Context, hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	s.called = "V4"
	return s.response, nil
}

func (s *getPayloadEngineStub) GetPayloadV5(context.Context, hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	s.called = "V5"
	return s.response, nil
}

func (s *getPayloadEngineStub) GetPayloadV6(context.Context, hexutil.Bytes) (*engine_types.GetPayloadResponse, error) {
	s.called = "V6"
	return s.response, nil
}

func TestGetAssembledBlockRoutesByVersion(t *testing.T) {
	for _, tc := range []struct {
		version clparams.StateVersion
		method  string
	}{
		{clparams.DenebVersion, "V3"},
		{clparams.ElectraVersion, "V4"},
		{clparams.FuluVersion, "V5"},
		{clparams.GloasVersion, "V6"},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			stub := &getPayloadEngineStub{response: &engine_types.GetPayloadResponse{
				ExecutionPayload: &engine_types.ExecutionPayload{},
				BlobsBundle:      &engine_types.BlobsBundle{},
			}}
			cc := &ExecutionClientEngine{engine: stub, beaconCfg: &cfg}

			_, _, _, _, err := cc.GetAssembledBlock(t.Context(), []byte{1}, tc.version)

			require.NoError(t, err)
			require.Equal(t, tc.method, stub.called)
		})
	}
}

func TestGetAssembledBlockRejectsMissingBlobsBundle(t *testing.T) {
	for _, version := range []clparams.StateVersion{
		clparams.DenebVersion,
		clparams.ElectraVersion,
		clparams.FuluVersion,
		clparams.GloasVersion,
	} {
		t.Run(version.String(), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cc := &ExecutionClientEngine{
				engine: &getPayloadEngineStub{response: &engine_types.GetPayloadResponse{
					ExecutionPayload: &engine_types.ExecutionPayload{},
				}},
				beaconCfg: &cfg,
			}

			payload, bundle, _, _, err := cc.GetAssembledBlock(t.Context(), []byte{1}, version)

			require.Nil(t, payload)
			require.Nil(t, bundle)
			require.ErrorIs(t, err, ErrInvalidGetPayloadResponse)
			require.ErrorContains(t, err, "missing blobs bundle")
		})
	}
}

func TestGetAssembledBlockRejectsMissingExecutionPayload(t *testing.T) {
	for _, version := range []clparams.StateVersion{
		clparams.DenebVersion,
		clparams.ElectraVersion,
		clparams.FuluVersion,
		clparams.GloasVersion,
	} {
		t.Run(version.String(), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cc := &ExecutionClientEngine{
				engine: &getPayloadEngineStub{response: &engine_types.GetPayloadResponse{
					BlobsBundle: &engine_types.BlobsBundle{},
				}},
				beaconCfg: &cfg,
			}

			payload, bundle, _, _, err := cc.GetAssembledBlock(t.Context(), []byte{1}, version)

			require.Nil(t, payload)
			require.Nil(t, bundle)
			require.ErrorIs(t, err, ErrInvalidGetPayloadResponse)
			require.ErrorContains(t, err, "nil execution payload")
		})
	}
}

func TestGetAssembledBlockAcceptsEmptyBlobsBundle(t *testing.T) {
	for _, version := range []clparams.StateVersion{
		clparams.DenebVersion,
		clparams.ElectraVersion,
		clparams.FuluVersion,
		clparams.GloasVersion,
	} {
		t.Run(version.String(), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			bundle := &engine_types.BlobsBundle{}
			cc := &ExecutionClientEngine{
				engine: &getPayloadEngineStub{response: &engine_types.GetPayloadResponse{
					ExecutionPayload: &engine_types.ExecutionPayload{},
					BlobsBundle:      bundle,
				}},
				beaconCfg: &cfg,
			}

			payload, gotBundle, _, _, err := cc.GetAssembledBlock(t.Context(), []byte{1}, version)

			require.NoError(t, err)
			require.NotNil(t, payload)
			require.Same(t, bundle, gotBundle)
		})
	}
}

// A forkchoice update that ran out of time has not been refused: the execution layer may still
// apply it, and no payload id came back. Reporting that as success left every caller to infer a
// timeout from an empty id, which reads identically to an execution layer that is syncing.
func TestForkChoiceUpdateReportsATimeoutRatherThanAnEmptySuccess(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"wrapped context deadline", fmt.Errorf("wrapped: %w", context.DeadlineExceeded)},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "context deadline exceeded")},
		{"legacy grpc string", errors.New("rpc error: code = DeadlineExceeded desc = context deadline exceeded")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cc := &ExecutionClientEngine{engine: &fcuEngineStub{err: tc.err}, beaconCfg: &cfg}

			id, err := cc.ForkChoiceUpdate(t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.DenebVersion)

			require.Nil(t, id)
			require.ErrorIs(t, err, ErrForkChoiceUpdateTimeout)
		})
	}
}

// A failure that is not a timeout keeps reporting as an ordinary failure, so a caller that retries
// only on timeouts does not retry a rejection forever.
func TestForkChoiceUpdateDoesNotCallEveryFailureATimeout(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cc := &ExecutionClientEngine{engine: &fcuEngineStub{err: errors.New("boom")}, beaconCfg: &cfg}

	_, err := cc.ForkChoiceUpdate(t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.DenebVersion)

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrForkChoiceUpdateTimeout)
}

func TestForkChoiceUpdateRejectsMissingPayloadIDForPayloadBuild(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cc := &ExecutionClientEngine{
		engine: &fcuEngineStub{response: &engine_types.ForkChoiceUpdatedResponse{
			PayloadStatus: &engine_types.PayloadStatus{Status: engine_types.SyncingStatus},
		}},
		beaconCfg: &cfg,
	}

	id, err := cc.ForkChoiceUpdate(
		t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, &engine_types.PayloadAttributes{}, clparams.DenebVersion,
	)

	require.ErrorIs(t, err, ErrForkChoiceUpdateNoPayloadID)
	require.Nil(t, id)
}

func TestForkChoiceUpdateAllowsMissingPayloadIDWithoutPayloadBuild(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cc := &ExecutionClientEngine{
		engine: &fcuEngineStub{response: &engine_types.ForkChoiceUpdatedResponse{
			PayloadStatus: &engine_types.PayloadStatus{Status: engine_types.SyncingStatus},
		}},
		beaconCfg: &cfg,
	}

	id, err := cc.ForkChoiceUpdate(
		t.Context(), common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.DenebVersion,
	)

	require.NoError(t, err)
	require.Empty(t, id)
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

func TestExecutionPayloadFromSSZBlock_TransactionsAreJSONArray(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	tests := []struct {
		name    string
		version clparams.StateVersion
		json    string
		want    []any
	}{
		{name: "pre-Gloas empty", version: clparams.ElectraVersion, json: "[]", want: []any{}},
		{name: "pre-Gloas non-empty", version: clparams.ElectraVersion, json: `["0x0102"]`, want: []any{"0x0102"}},
		{name: "Gloas empty", version: clparams.GloasVersion, json: "[]", want: []any{}},
		{name: "Gloas non-empty", version: clparams.GloasVersion, json: `["0x0102"]`, want: []any{"0x0102"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := cltypes.NewEth1Block(tt.version, &beaconCfg)
			payload.Extra = solid.NewExtraData()
			payload.Transactions = &solid.TransactionsSSZ{}
			payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)
			require.NoError(t, payload.Transactions.UnmarshalJSON([]byte(tt.json)))

			raw, err := json.Marshal(engine_types.ExecutionPayloadFromSSZBlock(payload, tt.version))
			require.NoError(t, err)

			var decoded map[string]any
			require.NoError(t, json.Unmarshal(raw, &decoded))
			require.Equal(t, tt.want, decoded["transactions"])
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
