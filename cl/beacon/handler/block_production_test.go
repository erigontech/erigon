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

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	builder_mock "github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestBlockBuilderWindowPreGloas(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.ElectraVersion, false)

	// Attestation deadline is 4s; polling stops a quarter of it (1s) earlier, at 3s.
	require.Equal(t, slotStart.Add(3*time.Second).Add(-minPayloadPollingWindow), window.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), window.pollUntil)
}

func TestBlockBuilderWindowGloas(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.GloasVersion, false)

	// Attestation deadline is 3s; polling stops a quarter of it (750ms) earlier, at 2.25s.
	require.Equal(t, slotStart.Add(2250*time.Millisecond).Add(-minPayloadPollingWindow), window.firstGetAt)
	require.Equal(t, slotStart.Add(2250*time.Millisecond), window.pollUntil)
}

func TestPublishBlindedBlocksRejectsGloas(t *testing.T) {
	_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	req := httptest.NewRequest(http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(nil))
	req.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())

	_, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), cltypes.ErrGloasCannotBlind.Error())
}

func TestPublishBlindedBlocksRejectsPreBellatrix(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.Phase0Version, clparams.AltairVersion} {
		t.Run(version.String(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := &ApiHandler{
				beaconChainCfg: &clparams.MainnetBeaconConfig,
				builderClient:  builder_mock.NewMockBuilderClient(ctrl),
			}
			block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, version)
			body, err := json.Marshal(block)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Eth-Consensus-Version", version.String())

			_, err = h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
			require.ErrorContains(t, err, "blinded blocks are unsupported before Bellatrix")
		})
	}

	require.NoError(t, validateBlindedBlockRequest(
		cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.BellatrixVersion),
		clparams.BellatrixVersion,
	))
}

func TestPublishBlindedBlocksRejectsUnsupportedContentType(t *testing.T) {
	h := &ApiHandler{beaconChainCfg: &clparams.MainnetBeaconConfig}
	req := httptest.NewRequest(http.MethodPost, "/eth/v2/beacon/blinded_blocks", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

	_, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	endpointErr, ok := err.(*beaconhttp.EndpointError)
	require.True(t, ok)
	require.Equal(t, http.StatusUnsupportedMediaType, endpointErr.Code)
	require.ErrorContains(t, err, "unsupported content type")
}

func TestPublishBlindedBlocksAcceptsEmptyFuluBuilderResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	builderClient := builder_mock.NewMockBuilderClient(ctrl)
	builderClient.EXPECT().SubmitBlindedBlocks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *cltypes.ExecutionRequests, error) {
			require.Equal(t, cltypes.NewEth1Header(clparams.FuluVersion).EncodingSizeSSZ(), block.Block.Body.ExecutionPayload.EncodingSizeSSZ())
			return nil, nil, nil, nil
		},
	)
	h := &ApiHandler{
		beaconChainCfg: &clparams.MainnetBeaconConfig,
		builderClient:  builderClient,
	}
	block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	body, err := json.Marshal(block)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

	resp, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestPublishBlindedBlocksRejectsMissingPreFuluPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	builderClient := builder_mock.NewMockBuilderClient(ctrl)
	builderClient.EXPECT().SubmitBlindedBlocks(gomock.Any(), gomock.Any()).Return(nil, nil, nil, nil)
	h := &ApiHandler{
		beaconChainCfg: &clparams.MainnetBeaconConfig,
		builderClient:  builderClient,
	}
	block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
	body, err := json.Marshal(block)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Eth-Consensus-Version", clparams.ElectraVersion.String())

	_, err = h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	require.ErrorContains(t, err, "builder returned nil execution payload")
}

func TestPublishBlindedBlocksRejectsMalformedRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*cltypes.SignedBlindedBeaconBlock)
		mutateJSON func(*testing.T, []byte) []byte
		wantErr    string
	}{
		{
			name: "missing block",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block = nil
			},
			wantErr: "missing block",
		},
		{
			name: "missing body",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block.Body = nil
			},
			wantErr: "missing block body",
		},
		{
			name: "null execution payload header",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block.Body.ExecutionPayload = nil
			},
			mutateJSON: func(t *testing.T, body []byte) []byte {
				var request map[string]any
				require.NoError(t, json.Unmarshal(body, &request))
				message := request["message"].(map[string]any)
				blockBody := message["body"].(map[string]any)
				blockBody["execution_payload_header"] = nil
				encoded, err := json.Marshal(request)
				require.NoError(t, err)
				return encoded
			},
			wantErr: "missing execution payload header",
		},
		{
			name: "omitted execution payload header",
			mutate: func(block *cltypes.SignedBlindedBeaconBlock) {
				block.Block.Body.ExecutionPayload = nil
			},
			wantErr: "missing execution payload header",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			builderClient := builder_mock.NewMockBuilderClient(ctrl)
			h := &ApiHandler{
				beaconChainCfg: &clparams.MainnetBeaconConfig,
				builderClient:  builderClient,
			}
			block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
			tc.mutate(block)
			body, err := json.Marshal(block)
			require.NoError(t, err)
			if tc.mutateJSON != nil {
				body = tc.mutateJSON(t, body)
			}
			req := httptest.NewRequest(http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

			_, err = h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateBuilderPayload(t *testing.T) {
	validPayload := func(version clparams.StateVersion) *cltypes.Eth1Block {
		payload := cltypes.NewEth1Block(version, &clparams.MainnetBeaconConfig)
		payload.Extra = solid.NewExtraData()
		payload.Transactions = &solid.TransactionsSSZ{}
		if version.AfterOrEqual(clparams.CapellaVersion) {
			payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
		}
		return payload
	}
	validBellatrix := validPayload(clparams.BellatrixVersion)
	require.NoError(t, validateBuilderPayload(validBellatrix, nil, clparams.BellatrixVersion))

	for _, tc := range []struct {
		name    string
		payload *cltypes.Eth1Block
		version clparams.StateVersion
		wantErr string
	}{
		{name: "missing payload", version: clparams.BellatrixVersion, wantErr: "nil execution payload"},
		{
			name: "missing extra data",
			payload: func() *cltypes.Eth1Block {
				payload := validPayload(clparams.BellatrixVersion)
				payload.Extra = nil
				return payload
			}(),
			version: clparams.BellatrixVersion,
			wantErr: "missing extra data",
		},
		{
			name: "missing transactions",
			payload: func() *cltypes.Eth1Block {
				payload := validPayload(clparams.BellatrixVersion)
				payload.Transactions = nil
				return payload
			}(),
			version: clparams.BellatrixVersion,
			wantErr: "missing transactions",
		},
		{
			name: "missing withdrawals",
			payload: func() *cltypes.Eth1Block {
				payload := validPayload(clparams.CapellaVersion)
				payload.Withdrawals = nil
				return payload
			}(),
			version: clparams.CapellaVersion,
			wantErr: "missing withdrawals",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, validateBuilderPayload(tc.payload, nil, tc.version), tc.wantErr)
		})
	}
}

func TestValidateBuilderPayloadRejectsOlderResponseVersion(t *testing.T) {
	payload := cltypes.NewEth1Block(clparams.BellatrixVersion, &clparams.MainnetBeaconConfig)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}

	require.ErrorContains(t, validateBuilderPayload(payload, nil, clparams.ElectraVersion), "version mismatch")
}

func TestValidateBuilderPayloadRejectsMissingElectraExecutionRequests(t *testing.T) {
	payload := cltypes.NewEth1Block(clparams.ElectraVersion, &clparams.MainnetBeaconConfig)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)

	require.ErrorContains(t, validateBuilderPayload(payload, nil, clparams.ElectraVersion), "missing execution requests")
	require.NoError(t, validateBuilderPayload(payload, cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.ElectraVersion), clparams.ElectraVersion))
}

func TestBlockBuilderWindowLateStartKeepsPublicationMargin(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart.Add(2950 * time.Millisecond)

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.ElectraVersion, false)

	// A late request clamps the first poll up to now but still stops at 3s, preserving the margin.
	require.Equal(t, now, window.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), window.pollUntil)
}

func TestBlockBuilderWindowLateRequestGrabsImmediately(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)
	now := slotStart.Add(5 * time.Second)

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.GloasVersion, false)

	require.Equal(t, now, window.firstGetAt)
	require.Equal(t, now, window.pollUntil)
}

func TestBlockBuilderWindowReservesPublicationMargin(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)

	for _, tc := range []struct {
		name          string
		version       clparams.StateVersion
		deadline      time.Duration
		wantPollUntil time.Duration
	}{
		{"pre-gloas", clparams.ElectraVersion, 4 * time.Second, 3 * time.Second},
		{"gloas", clparams.GloasVersion, 3 * time.Second, 2250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := computeBlockBuilderWindow(slotStart, slotStart, cfg, tc.version, false)
			require.Equal(t, slotStart.Add(tc.wantPollUntil), window.pollUntil)
			require.True(t, window.pollUntil.Before(slotStart.Add(tc.deadline)),
				"polling must stop before the attestation deadline to leave publication margin")
		})
	}
}

func TestShouldRetryGetPayloadStopsAtDeadline(t *testing.T) {
	deadline := time.Unix(100, 0)

	require.True(t, shouldRetryGetPayload(deadline.Add(-time.Nanosecond), deadline))
	require.False(t, shouldRetryGetPayload(deadline, deadline))
	require.False(t, shouldRetryGetPayload(deadline.Add(time.Nanosecond), deadline))
}

func TestPollAssembledPayloadReturnsReadyPayload(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(-time.Millisecond), pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	payload, _, _, _, ok := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return want, nil, nil, nil, nil
		})
	require.True(t, ok)
	require.Same(t, want, payload)
	require.Equal(t, 1, calls)
}

func TestPollAssembledPayloadRetriesWhileBusy(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	payload, _, _, _, ok := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls < 3 {
				return nil, nil, nil, nil, nil
			}
			return want, nil, nil, nil, nil
		})
	require.True(t, ok)
	require.Same(t, want, payload)
	require.Equal(t, 3, calls)
}

func TestPollAssembledPayloadRetriesOnError(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	payload, _, _, _, ok := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls == 1 {
				return nil, nil, nil, nil, errors.New("EL busy")
			}
			return want, nil, nil, nil, nil
		})
	require.True(t, ok)
	require.Same(t, want, payload)
	require.Equal(t, 2, calls)
}

func TestPollAssembledPayloadStopsAtDeadline(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(50 * time.Millisecond)}
	calls := 0
	payload, _, _, _, ok := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.False(t, ok)
	require.Nil(t, payload)
	require.NotZero(t, calls)
}

func TestPollAssembledPayloadLateRequestGrabsOnce(t *testing.T) {
	past := time.Now().Add(-time.Second)
	window := blockBuilderWindow{firstGetAt: past, pollUntil: past}
	calls := 0
	_, _, _, _, ok := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.False(t, ok)
	require.Equal(t, 1, calls)
}

func TestPollAssembledPayloadReturnsOnContextCancel(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(time.Hour), pollUntil: now.Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	_, _, _, _, ok := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.False(t, ok)
	require.Zero(t, calls)
}

func TestSetupHeaderResponseForBlockProductionGloasPayloadIncluded(t *testing.T) {
	h := &ApiHandler{}
	rr := httptest.NewRecorder()

	h.setupHeaderReponseForBlockProduction(rr, clparams.GloasVersion, false, true, 123, 456)

	require.Equal(t, "gloas", rr.Header().Get("Eth-Consensus-Version"))
	require.Equal(t, "123", rr.Header().Get("Eth-Execution-Payload-Value"))
	require.Equal(t, "456", rr.Header().Get("Eth-Consensus-Block-Value"))
	require.Equal(t, "false", rr.Header().Get("Eth-Execution-Payload-Blinded"))
	require.Equal(t, "true", rr.Header().Get("Eth-Execution-Payload-Included"))
}

func TestSetupHeaderResponseForBlockProductionPreGloasOmitsPayloadIncluded(t *testing.T) {
	h := &ApiHandler{}
	rr := httptest.NewRecorder()

	h.setupHeaderReponseForBlockProduction(rr, clparams.ElectraVersion, false, true, 123, 456)

	require.Empty(t, rr.Header().Get("Eth-Execution-Payload-Included"))
}

// TestCaplinBlockProductionWithWithdrawalRequest tests Caplin's produceBeaconBody
// against a real Erigon execution layer. A withdrawal request transaction is
// submitted to the EIP-7002 system contract, and then Caplin's actual block
// production code builds the beacon body — calling ForkChoiceUpdate,
// GetAssembledBlock, and decoding the execution requests. This is the code path
// that was broken in issue #14319 and fixed in PR #14326.
func TestCaplinBlockProductionWithWithdrawalRequest(t *testing.T) {
	ctx := context.Background()

	// --- Set up real execution layer ---

	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	// Insert 1 initial block so we have a chain head.
	chainPack, err := m.GenerateChain(1, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Submit a withdrawal request transaction (EIP-7002).
	var pubkey [48]byte
	for i := range pubkey {
		pubkey[i] = 0x01
	}
	var calldata []byte
	calldata = append(calldata, pubkey[:]...)
	calldata = append(calldata, make([]byte, 8)...) // amount=0 → full exit

	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	withdrawalAddr := params.WithdrawalRequestAddress.Value()
	withdrawalTx, err := types.SignTx(
		&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 1_000_000,
				To:       &withdrawalAddr,
				Value:    *uint256.NewInt(500_000_000_000_000_000), // 0.5 ETH
				Data:     calldata,
			},
			GasPrice: *uint256.NewInt(baseFee),
		},
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)

	var txBuf bytes.Buffer
	err = withdrawalTx.EncodeRLP(&txBuf)
	require.NoError(t, err)
	addResp, err := m.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{txBuf.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, "success", addResp.Errors[0])

	// --- Wire real EL into Caplin's ApiHandler ---

	chainRW := chainreader.NewChainReaderEth1(
		m.ChainConfig,
		m.ExecModule,
		time.Hour,
	)
	engine, err := execution_client.NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)

	// Set up handler with Electra test data (provides validator set, RANDAO, etc.)
	// and our real execution engine.
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	h.engine = engine

	// Patch the beacon state's execution payload header to point at the real
	// EL chain head — this is how produceBeaconBody knows what hash to send
	// in ForkChoiceUpdate.
	elHead := chainPack.TopBlock.Header()
	elHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
	elHeader.BlockHash = elHead.Hash()
	elHeader.BlockNumber = elHead.Number.Uint64()
	elHeader.Time = elHead.Time
	elHeader.BaseFeePerGas = common.BigToHash(elHead.BaseFee.ToBig())
	postState.SetLatestExecutionPayloadHeader(elHeader)

	// Make GetEth1Hash return the EL head hash for any checkpoint root —
	// produceBeaconBody falls back to head when the hash is zero, but we
	// set it explicitly for clarity.
	elHeadHash := elHead.Hash()
	fcu.Eth1Hashes[postState.FinalizedCheckpoint().Root] = elHeadHash
	fcu.Eth1Hashes[postState.CurrentJustifiedCheckpoint().Root] = elHeadHash

	// --- Call Caplin's actual block production ---

	baseBlock := blocks[len(blocks)-1].Block
	targetSlot := baseBlock.Slot + 1
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	beaconBody, execValue, err := h.produceBeaconBody(
		ctx, 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{0xc0}, // infinity BLS signature (skip RANDAO verification)
		common.Hash{},
	)
	require.NoError(t, err)
	require.NotNil(t, beaconBody)
	require.NotZero(t, execValue)

	// --- Verify execution requests were decoded by Caplin ---

	require.NotNil(t, beaconBody.ExecutionRequests,
		"ExecutionRequests must not be nil — this was the bug in issue #14319")
	require.Greater(t, beaconBody.ExecutionRequests.Withdrawals.Len(), 0,
		"expected at least 1 withdrawal request from the EL system contract")

	gotWithdrawal := beaconBody.ExecutionRequests.Withdrawals.Get(0)
	require.Equal(t, common.Bytes48(pubkey), gotWithdrawal.ValidatorPubKey,
		"withdrawal request pubkey should match what was submitted")
	require.Equal(t, uint64(0), gotWithdrawal.Amount,
		"withdrawal request amount should be 0 (full exit)")
}

// fcuSpy wraps an ExecutionEngine and captures the PayloadAttributes from
// the most recent ForkChoiceUpdate call.
type fcuSpy struct {
	execution_client.ExecutionEngine
	lastAttributes *engine_types.PayloadAttributes
}

func (s *fcuSpy) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attributes *engine_types.PayloadAttributes, version clparams.StateVersion) ([]byte, error) {
	s.lastAttributes = attributes
	return s.ExecutionEngine.ForkChoiceUpdate(ctx, finalized, safe, head, attributes, version)
}

// TestCaplinBlockProductionGlamsterdamSlotNumber verifies that Caplin passes
// the slot number to the execution engine in PayloadAttributes when the
// Glamsterdam (Gloas) fork is active, per EIP-7843.
func TestCaplinBlockProductionGlamsterdamSlotNumber(t *testing.T) {
	ctx := context.Background()

	// --- Set up real execution layer with Amsterdam activated ---

	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	// Insert 1 initial block so we have a chain head.
	chainPack, err := m.GenerateChain(1, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// --- Wire real EL into Caplin's ApiHandler ---

	chainRW := chainreader.NewChainReaderEth1(
		m.ChainConfig,
		m.ExecModule,
		time.Hour,
	)
	engine, err := execution_client.NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)

	// Wrap the real engine with a spy to capture PayloadAttributes.
	spy := &fcuSpy{ExecutionEngine: engine}

	// Set up handler with Electra test data (provides validator set, RANDAO,
	// etc.) and plug in our spy engine.
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	h.engine = spy

	// Activate Fulu and Gloas at epoch 1 (same as the other forks in the
	// Electra fixture setup) so GetCurrentStateVersion returns GloasVersion.
	h.beaconChainCfg.FuluForkEpoch = 1
	h.beaconChainCfg.GloasForkEpoch = 1
	h.beaconChainCfg.InitializeForkSchedule()

	// Patch the beacon state's execution payload header to point at the real
	// EL chain head.
	elHead := chainPack.TopBlock.Header()
	elHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
	elHeader.BlockHash = elHead.Hash()
	elHeader.BlockNumber = elHead.Number.Uint64()
	elHeader.Time = elHead.Time
	elHeader.BaseFeePerGas = common.BigToHash(elHead.BaseFee.ToBig())
	postState.SetLatestExecutionPayloadHeader(elHeader)
	// GLOAS uses GetLatestBlockHash() instead of LatestExecutionPayloadHeader().BlockHash
	postState.SetLatestBlockHash(elHead.Hash())
	// GLOAS deferred payload: set LatestExecutionPayloadBid so that GetHeadPayloadStatus()==FULL &&
	// ShouldBuildOnFull (both returning true in the mock) select bid.BlockHash as the EL head.
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		BlockHash:       elHead.Hash(),
		ParentBlockHash: elHead.Hash(),
	})

	elHeadHash := elHead.Hash()
	fcu.Eth1Hashes[postState.FinalizedCheckpoint().Root] = elHeadHash
	fcu.Eth1Hashes[postState.CurrentJustifiedCheckpoint().Root] = elHeadHash

	// --- Call Caplin's actual block production ---

	baseBlock := blocks[len(blocks)-1].Block
	targetSlot := baseBlock.Slot + 1
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	// GLOAS deferred payload: the mock returns GetHeadPayloadStatus=FULL and ShouldBuildOnFull=true,
	// so block production expects an envelope on disk. Provide one with empty ExecutionRequests.
	fcu.Envelopes[baseBlockRoot] = &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(h.beaconChainCfg, clparams.GloasVersion),
		},
	}

	beaconBody, _, err := h.produceBeaconBody(
		ctx, 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{0xc0}, // infinity BLS signature (skip RANDAO verification)
		common.Hash{},
	)
	require.NoError(t, err)
	require.NotNil(t, beaconBody)

	// --- Verify the slot number was passed to the EL (EIP-7843) ---

	require.NotNil(t, spy.lastAttributes,
		"ForkChoiceUpdate should have been called with PayloadAttributes")
	require.NotNil(t, spy.lastAttributes.SlotNumber,
		"PayloadAttributes.SlotNumber must be set for Glamsterdam (EIP-7843)")
	require.Equal(t, hexutil.Uint64(targetSlot), *spy.lastAttributes.SlotNumber,
		"SlotNumber should equal the target slot")
}

func TestBlockBuilderWindowTakesPreparedPayloadEarly(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)

	// A primed payload is taken one quarter of the attestation deadline into the slot.
	prepared := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, true)
	require.Equal(t, slotStart.Add(time.Second).Add(-minPayloadPollingWindow), prepared.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), prepared.pollUntil)

	// Without a primed builder nothing changes: the execution layer still needs most of the slot.
	unprepared := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, false)
	require.Equal(t, slotStart.Add(3*time.Second).Add(-minPayloadPollingWindow), unprepared.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), unprepared.pollUntil)

	require.True(t, prepared.firstGetAt.Before(unprepared.firstGetAt))

	late := computeBlockBuilderWindow(slotStart.Add(2*time.Second), slotStart, cfg, clparams.ElectraVersion, true)
	require.Equal(t, slotStart.Add(2*time.Second), late.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), late.pollUntil)
}

func TestPreparedPayloadMatchesOnlyTheSamePrime(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)

	require.False(t, p.matches(10, id, now, 0), "nothing primed yet")

	p.set(10, id, now)
	require.True(t, p.matches(10, id, now, 0))

	// A different payload id means the execution layer started a fresh build — a reorg, a late
	// block, or a changed fee recipient — so the warm builder is gone.
	require.False(t, p.matches(10, []byte{9, 9, 9, 9, 9, 9, 9, 9}, now, 0))
	require.False(t, p.matches(11, id, now, 0), "primed for another slot")
	require.False(t, p.matches(10, nil, now, 0), "no id from the execution layer")
}

func TestPreparedPayloadRequiresMinimumWarmup(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)
	minAge := 2 * time.Second

	p.set(10, id, now.Add(-minAge+time.Nanosecond))
	require.False(t, p.matches(10, id, now, minAge))

	p.set(10, id, now.Add(-minAge))
	require.True(t, p.matches(10, id, now, minAge))
}

func TestPreparedPayloadMinimumWarmupPreservesBuildTime(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}

	require.Equal(t, 2*time.Second, preparedPayloadMinimumAge(cfg, clparams.ElectraVersion))
}

func TestPreparedPayloadRequiresBuilderContinuity(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)
	p.set(10, id, now.Add(-3*time.Second))

	require.True(t, canUsePreparedPayload(&p, true, 10, id, now, 2*time.Second))
	require.False(t, canUsePreparedPayload(&p, false, 10, id, now, 2*time.Second))
}

func TestStartPayloadPreparationSkipsRemoteEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(false)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: true},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handler.StartPayloadPreparation(ctx)
}

func TestStartPayloadPreparationSkipsWithoutValidatorAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Times(0)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: false},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
	}

	handler.StartPayloadPreparation(t.Context())
}

func TestStartPayloadPreparationSkipsNilEngine(t *testing.T) {
	handler := &ApiHandler{}
	require.NotPanics(t, func() {
		handler.StartPayloadPreparation(t.Context())
	})
}

func TestStartPayloadPreparationStartsLocalEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(true)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: true},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	handler.StartPayloadPreparation(ctx)
}

func TestPreparePayloadLoopRunsImmediatelyWithSlotDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	slotStart := time.Now().Add(6 * time.Second)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1)
	clock.EXPECT().GetSlotTime(targetSlot).Return(slotStart).AnyTimes()
	handler.ethClock = clock

	ctx, cancel := context.WithCancel(t.Context())
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(callCtx context.Context, _, _, _ common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			deadline, ok := callCtx.Deadline()
			require.True(t, ok)
			require.Equal(t, slotStart, deadline)
			cancel()
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler.engine = engine

	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopSkipsSlotsTooFarAhead(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// Before genesis the current slot clamps to zero, so the next slot can be hours out.
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(time.Hour)).AnyTimes()
	handler.ethClock = clock

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(true)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.StartPayloadPreparation(ctx)
	<-ctx.Done()
}

func TestPreparePayloadLoopStandsOffWhileProducing(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Times(0)
	handler.ethClock = clock

	// Priming would contend with the block being produced for the execution layer's single slot.
	handler.proposalsInFlight.Add(1)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopSkipsSlotsAboutToStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// Too close to the slot for the prime to ever reach the age production demands of it, so the
	// state copy and the forkchoice update would both be spent for nothing.
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(200 * time.Millisecond)).AnyTimes()
	handler.ethClock = clock

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestForkChoiceUpdateForProposalWaitsOutContention(t *testing.T) {
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(uint64(10)).Return(time.Now())

	calls := 0
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
			calls++
			if calls < 3 {
				return nil, execution_client.ErrForkChoiceBusy
			}
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		}).Times(3)

	a := &ApiHandler{engine: engine, ethClock: clock, beaconChainCfg: &clparams.MainnetBeaconConfig}
	id, err := a.forkChoiceUpdateForProposal(t.Context(), 10, common.Hash{}, common.Hash{}, common.Hash{}, &engine_types.PayloadAttributes{}, clparams.ElectraVersion)

	require.NoError(t, err)
	require.Len(t, id, 8)
}

func TestForkChoiceUpdateForProposalStopsOnceTheWindowClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(uint64(10)).Return(time.Now().Add(-time.Minute))

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, execution_client.ErrForkChoiceBusy).Times(1)

	// Past the point the payload has to be collected, another attempt cannot help this slot.
	a := &ApiHandler{engine: engine, ethClock: clock, beaconChainCfg: &clparams.MainnetBeaconConfig}
	_, err := a.forkChoiceUpdateForProposal(t.Context(), 10, common.Hash{}, common.Hash{}, common.Hash{}, &engine_types.PayloadAttributes{}, clparams.ElectraVersion)

	require.ErrorIs(t, err, execution_client.ErrForkChoiceBusy)
}

func TestPreparePayloadForSendsCompleteForkChoiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, fcu, validatorParams := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)
	config := *handler.beaconChainCfg
	targetEpoch := postState.Slot()/config.SlotsPerEpoch + 1
	targetSlot := targetEpoch * config.SlotsPerEpoch
	config.DenebForkEpoch = targetEpoch
	config.InitializeForkSchedule()

	headState := state.New(&config)
	require.NoError(t, postState.CopyInto(headState))
	headState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: targetEpoch - 2, Root: common.Hash{0x31}})
	headState.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: targetEpoch - 1, Root: common.Hash{0x32}})
	currentEpoch := headState.Slot() / config.SlotsPerEpoch
	headState.SetRandaoMixAt(int(currentEpoch%config.EpochsPerHistoricalVector), common.Hash{0x51})
	headState.SetRandaoMixAt(int(targetEpoch%config.EpochsPerHistoricalVector), common.Hash{0x52})
	baseBlockRoot := common.Hash{0x41}
	syncedData := synced_data.NewSyncedDataManager(&config, true)
	require.NoError(t, syncedData.OnHeadStateWithBlockRoot(headState, baseBlockRoot))
	syncedData.OnSelectedHead(baseBlockRoot, headState.Slot())
	handler.beaconChainCfg = &config
	handler.syncedData = syncedData

	proposerIndex, err := headState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)

	feeRecipient := common.Address{0x11}
	validatorParams.SetFeeRecipient(proposerIndex, feeRecipient)

	advancedState, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
	require.Equal(t, clparams.CapellaVersion, headState.Version())
	require.Equal(t, clparams.DenebVersion, advancedState.Version())
	require.NotEqual(t, headState.GetRandaoMixes(targetEpoch), advancedState.GetRandaoMixes(targetEpoch))
	finalizedRoot := advancedState.FinalizedCheckpoint().Root
	justifiedRoot := advancedState.CurrentJustifiedCheckpoint().Root
	require.NotEqual(t, finalizedRoot, justifiedRoot)
	expectedFinalized := common.Hash{0x21}
	expectedSafe := common.Hash{0x22}
	fcu.Eth1Hashes[finalizedRoot] = expectedFinalized
	fcu.Eth1Hashes[justifiedRoot] = expectedSafe
	require.NotEqual(t, expectedFinalized, expectedSafe)

	version := handler.beaconChainCfg.GetCurrentStateVersion(targetSlot / handler.beaconChainCfg.SlotsPerEpoch)
	require.Equal(t, clparams.DenebVersion, version)
	expectedWithdrawals, err := state.GetExpectedWithdrawals(advancedState, targetSlot/handler.beaconChainCfg.SlotsPerEpoch)
	require.NoError(t, err)

	payloadID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, finalized, safe, head common.Hash, attrs *engine_types.PayloadAttributes, gotVersion clparams.StateVersion) ([]byte, error) {
			require.Equal(t, expectedFinalized, finalized)
			require.Equal(t, expectedSafe, safe)
			require.Equal(t, advancedState.LatestExecutionPayloadHeader().BlockHash, head)
			require.Equal(t, version, gotVersion)
			require.Equal(t, hexutil.Uint64(state.ComputeTimestampAtSlot(advancedState, targetSlot)), attrs.Timestamp)
			require.Equal(t, common.Hash(advancedState.GetRandaoMixes(targetSlot/handler.beaconChainCfg.SlotsPerEpoch)), attrs.PrevRandao)
			require.Equal(t, feeRecipient, attrs.SuggestedFeeRecipient)
			require.Equal(t, &baseBlockRoot, attrs.ParentBeaconBlockRoot)
			require.Nil(t, attrs.SlotNumber)
			require.Nil(t, attrs.TargetGasLimit)
			require.NotNil(t, attrs.Withdrawals)
			require.Len(t, attrs.Withdrawals, len(expectedWithdrawals.Withdrawals))
			for i, withdrawal := range expectedWithdrawals.Withdrawals {
				require.Equal(t, withdrawal.Index, attrs.Withdrawals[i].Index)
				require.Equal(t, withdrawal.Amount, attrs.Withdrawals[i].Amount)
				require.Equal(t, withdrawal.Validator, attrs.Withdrawals[i].Validator)
				require.Equal(t, withdrawal.Address, attrs.Withdrawals[i].Address)
			}
			return payloadID, nil
		})
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	primedHead, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
	require.Equal(t, baseBlockRoot, primedHead)
	require.True(t, handler.preparedPayload.matches(targetSlot, payloadID, time.Now(), 0))
}

func TestPreparePayloadForRejectsChangedHeadBeforeForkChoiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	changedBlockRoot := common.Hash{0x42}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	gomock.InOrder(
		syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
			DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
				return view(postState, baseBlockRoot, postState.Slot())
			}),
		syncedDataMock.EXPECT().SelectedHead().Return(changedBlockRoot, postState.Slot(), true),
	)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = handler.preparePayloadFor(t.Context(), targetSlot)
	require.ErrorIs(t, err, errPreparationHeadChanged)
	require.False(t, handler.preparedPayload.matches(targetSlot, []byte{1}, time.Now(), 0))
}

func TestPreparePayloadForUsesPostEpochProposer(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	config := *handler.beaconChainCfg
	targetEpoch := postState.Slot()/config.SlotsPerEpoch + 1
	targetSlot := targetEpoch * config.SlotsPerEpoch
	config.FuluForkEpoch = targetEpoch + 10
	config.GloasForkEpoch = targetEpoch + 11
	config.InitializeForkSchedule()
	handler.beaconChainCfg = &config

	headState := state.New(&config)
	require.NoError(t, postState.CopyInto(headState))
	headState.SetSlot(targetSlot - 1)
	for i := 0; i < headState.ValidatorLength(); i += 2 {
		headState.SetEffectiveBalanceForValidatorAtIndex(i, 0)
		require.NoError(t, headState.SetValidatorBalance(i, config.MaxEffectiveBalanceElectra))
	}

	mixPosition := (targetEpoch + config.EpochsPerHistoricalVector - config.MinSeedLookahead - 1) % config.EpochsPerHistoricalVector
	var oldProposer, newProposer uint64
	found := false
	for nonce := byte(0); ; nonce++ {
		headState.SetRandaoMixAt(int(mixPosition), common.Hash{nonce})
		var err error
		oldProposer, err = headState.GetBeaconProposerIndexForSlot(targetSlot)
		require.NoError(t, err)

		advanced, err := headState.Copy()
		require.NoError(t, err)
		require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, targetSlot))
		newProposer, err = advanced.GetBeaconProposerIndexForSlot(targetSlot)
		require.NoError(t, err)
		if oldProposer != newProposer {
			found = true
			break
		}
		if nonce == 255 {
			break
		}
	}
	require.True(t, found, "fixture must expose a proposer change across epoch processing")
	validatorParams.SetFeeRecipient(newProposer, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(headState, baseBlockRoot, headState.Slot())
		})
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, headState.Slot(), true)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.Equal(t, common.Address{0x11}, attrs.SuggestedFeeRecipient)
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
}

func TestPreparePayloadForPairsRootAndStateFromOneView(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// The root the view hands over is the only one preparation may use. Reading it separately would
	// let a head update in between pair a parent beacon block root with a different state, priming a
	// builder production can never match.
	viewRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, viewRoot, postState.Slot())
		})
	syncedDataMock.EXPECT().SelectedHead().Return(viewRoot, postState.Slot(), true)

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.NotNil(t, attrs.ParentBeaconBlockRoot)
			require.Equal(t, viewRoot, *attrs.ParentBeaconBlockRoot)
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler.engine = engine

	primedHead, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
	require.Equal(t, viewRoot, primedHead)
}

func TestGetEthV3ValidatorBlockMapsNotSyncedToServiceUnavailable(t *testing.T) {
	_, _, _, _, _, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).Return(synced_data.ErrNotSynced)

	req := httptest.NewRequest(http.MethodGet, "/?randao_reveal="+hexutil.Encode(make([]byte, 96)), nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("slot", "1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))

	_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), req)
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusServiceUnavailable, endpointErr.Code)
}

func TestShouldPreparePayloadVersion(t *testing.T) {
	for _, tc := range []struct {
		version clparams.StateVersion
		want    bool
	}{
		{clparams.Phase0Version, false},
		{clparams.AltairVersion, false},
		{clparams.BellatrixVersion, false},
		{clparams.CapellaVersion, true},
		{clparams.DenebVersion, true},
		{clparams.FuluVersion, true},
		{clparams.GloasVersion, false},
	} {
		require.Equal(t, tc.want, shouldPreparePayloadVersion(tc.version), tc.version.String())
	}
}

func TestPayloadAttributesCarryEveryFieldTheExecutionLayerGates(t *testing.T) {
	root := common.Hash{0xaa}
	withdrawals := []*types.Withdrawal{{Index: 1}}

	attrs := payloadAttributes(1, common.Hash{0xbb}, common.Address{0xcc}, withdrawals, &root)

	require.Equal(t, withdrawals, attrs.Withdrawals)
	require.Equal(t, &root, attrs.ParentBeaconBlockRoot)
	require.Nil(t, attrs.SlotNumber, "only a Gloas proposal knows its slot number")
	require.Nil(t, attrs.TargetGasLimit)
}

func TestPreparedPayloadKeepsConsecutiveSlots(t *testing.T) {
	var p preparedPayload
	first := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	second := []byte{2, 2, 2, 2, 2, 2, 2, 2}
	now := time.Unix(100, 0)

	// Consecutive proposals: priming slot 11 must not evict slot 10, whose block may still be
	// in production.
	p.set(10, first, now)
	p.set(11, second, now)
	require.True(t, p.matches(10, first, now, 0))
	require.True(t, p.matches(11, second, now, 0))

	// Records old enough that they can no longer be produced are dropped, so the map is bounded.
	p.set(10+preparedPayloadRetainSlots+1, []byte{3, 3, 3, 3, 3, 3, 3, 3}, now)
	require.False(t, p.matches(10, first, now, 0))
}

func TestPreparedPayloadCopiesTheID(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)

	p.set(10, id, now)
	id[0] = 0xff

	// The caller's buffer must not be able to invalidate, or forge, a later match.
	require.True(t, p.matches(10, []byte{1, 2, 3, 4, 5, 6, 7, 8}, now, 0))
	require.False(t, p.matches(10, id, now, 0))
}

func TestSlotHeadMatchesOnlyTheExactPairing(t *testing.T) {
	headA := common.Hash{0xaa}
	headB := common.Hash{0xbb}
	settled := slotHead{slot: 10, head: headA, generation: 3}

	// Nothing recorded yet matches nothing.
	require.NotEqual(t, settled, slotHead{})

	// Already settled for this slot, on this head, under these registrations: nothing to do.
	require.Equal(t, settled, slotHead{slot: 10, head: headA, generation: 3})

	// The previous slot's block arrived late and moved the head, so the warm builder is on a parent
	// that is no longer the head — prime again rather than wait for the next slot.
	require.NotEqual(t, settled, slotHead{slot: 10, head: headB, generation: 3})

	// A new target slot always needs priming.
	require.NotEqual(t, settled, slotHead{slot: 11, head: headA, generation: 3})

	// A registration arriving late can make a slot ours after it was ruled out, or change the fee
	// recipient a prime already went out with.
	require.NotEqual(t, settled, slotHead{slot: 10, head: headA, generation: 4})
}
