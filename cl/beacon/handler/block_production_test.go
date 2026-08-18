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
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	builder_mock "github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
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

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.ElectraVersion)

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

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.GloasVersion)

	// Attestation deadline is 3s; polling stops a quarter of it (750ms) earlier, at 2.25s.
	require.Equal(t, slotStart.Add(2250*time.Millisecond).Add(-minPayloadPollingWindow), window.firstGetAt)
	require.Equal(t, slotStart.Add(2250*time.Millisecond), window.pollUntil)
}

func TestPublishBlindedBlocksRejectsGloas(t *testing.T) {
	_, _, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(nil))
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
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
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
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", nil)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

	_, err := h.publishBlindedBlocks(httptest.NewRecorder(), req, 2)
	var endpointErr *beaconhttp.EndpointError
	require.True(t, errors.As(err, &endpointErr))
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
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
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
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
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
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blinded_blocks", bytes.NewReader(body))
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

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.ElectraVersion)

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

	window := computeBlockBuilderWindow(now, slotStart, cfg, clparams.GloasVersion)

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
			window := computeBlockBuilderWindow(slotStart, slotStart, cfg, tc.version)
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
	result, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return want, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Same(t, want, result.payload)
	require.Equal(t, 1, calls)
}

func TestPollAssembledPayloadRetriesWhileBusy(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	result, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls < 3 {
				return nil, nil, nil, nil, nil
			}
			return want, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Same(t, want, result.payload)
	require.Equal(t, 3, calls)
}

func TestPollAssembledPayloadRetriesOnError(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}
	want := &cltypes.Eth1Block{}
	calls := 0
	result, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls == 1 {
				return nil, nil, nil, nil, errors.New("EL busy")
			}
			return want, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Same(t, want, result.payload)
	require.Equal(t, 2, calls)
}

func TestPollAssembledPayloadStopsAtDeadline(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(50 * time.Millisecond)}
	calls := 0
	result, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Nil(t, result)
	require.NotZero(t, calls)
}

func TestPollAssembledPayloadLateRequestGrabsOnce(t *testing.T) {
	past := time.Now().Add(-time.Second)
	window := blockBuilderWindow{firstGetAt: past, pollUntil: past}
	calls := 0
	result, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.NoError(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, calls)
}

func TestPollAssembledPayloadReturnsOnContextCancel(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(time.Hour), pollUntil: now.Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	result, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			return nil, nil, nil, nil, nil
		})
	require.Nil(t, result)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestPollAssembledPayloadPrefersCancellationOverExpiredPollingWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	past := time.Now().Add(-time.Second)
	window := blockBuilderWindow{firstGetAt: past, pollUntil: past}

	for range 100 {
		result, err := pollAssembledPayload(ctx, window, time.Hour,
			func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
				return nil, nil, nil, nil, nil
			})

		require.Nil(t, result)
		require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
		require.ErrorIs(t, err, context.Canceled)
	}
}

type getPayloadErrorLogCounter struct{ errors int }

func (h *getPayloadErrorLogCounter) Log(r *log.Record) error {
	if r.Lvl <= log.LvlError && r.Msg == "BlockProduction: Failed to get payload" {
		h.errors++
	}
	return nil
}

func (h *getPayloadErrorLogCounter) Enabled(context.Context, log.Lvl) bool { return true }

func captureGetPayloadErrors(t *testing.T) *getPayloadErrorLogCounter {
	t.Helper()
	rec := &getPayloadErrorLogCounter{}
	prevHandler := log.Root().GetHandler()
	log.Root().SetHandler(rec)
	t.Cleanup(func() { log.Root().SetHandler(prevHandler) })
	return rec
}

func TestPollAssembledPayloadDoesNotReportCancellationAsError(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(-time.Millisecond), pollUntil: now.Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())

	rec := captureGetPayloadErrors(t)

	calls := 0
	result, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			if calls == 1 {
				return nil, nil, nil, nil, errors.New("EL hiccup")
			}
			cancel()
			return nil, nil, nil, nil, fmt.Errorf("%w: %w", execmodule.ErrRequestAbandoned, ctx.Err())
		})

	require.Nil(t, result)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, calls, "polling must stop once the request is cancelled")
	require.Equal(t, 1, rec.errors, "only the failure before the cancellation is a node fault")
}

func TestPollAssembledPayloadReportsFailureAfterCancellation(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(-time.Millisecond), pollUntil: now.Add(time.Hour)}
	ctx, cancel := context.WithCancel(context.Background())
	failure := fmt.Errorf("EL disconnected: %w", context.Canceled)

	rec := captureGetPayloadErrors(t)

	calls := 0
	result, err := pollAssembledPayload(ctx, window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			calls++
			cancel()
			return nil, nil, nil, nil, failure
		})

	require.Nil(t, result)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, rec.errors, "an EL failure with its own context error must remain visible")
}

func TestPollAssembledPayloadDoesNotReportBusyAsError(t *testing.T) {
	now := time.Now()
	window := blockBuilderWindow{firstGetAt: now.Add(-time.Millisecond), pollUntil: now.Add(5 * time.Millisecond)}
	rec := captureGetPayloadErrors(t)

	result, err := pollAssembledPayload(context.Background(), window, time.Millisecond,
		func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			return nil, nil, nil, nil, execmodule.ErrBusy
		})

	require.Nil(t, result)
	require.ErrorIs(t, err, execmodule.ErrBusy)
	require.Zero(t, rec.errors)
}

func TestPollAssembledPayloadDoesNotReportStaleBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		window := blockBuilderWindow{firstGetAt: now, pollUntil: now.Add(time.Second)}

		calls := 0
		result, err := pollAssembledPayload(t.Context(), window, time.Millisecond,
			func() (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
				calls++
				if calls == 1 {
					return nil, nil, nil, nil, execmodule.ErrBusy
				}
				return nil, nil, nil, nil, nil
			})

		require.Nil(t, result)
		require.NoError(t, err)
		require.Greater(t, calls, 1)
	})
}

type abandonedBlockProductionEngine struct {
	execution_client.ExecutionEngine
}

func (abandonedBlockProductionEngine) ForkChoiceUpdate(
	context.Context,
	common.Hash,
	common.Hash,
	common.Hash,
	*engine_types.PayloadAttributes,
	clparams.StateVersion,
) ([]byte, error) {
	return nil, execmodule.RequestAbandonedError(context.Canceled, execmodule.ErrBusy)
}

type blockProductionFailureLogCounter struct {
	mu                     sync.Mutex
	failures               int
	busyDiagnostics        int
	abandonmentDiagnostics int
	unavailableWarnings    int
}

func (h *blockProductionFailureLogCounter) Log(r *log.Record) error {
	if r.Msg == "Block production request ended while execution module was busy" {
		h.mu.Lock()
		h.busyDiagnostics++
		h.mu.Unlock()
	}
	if r.Msg == "Block production request abandoned" {
		h.mu.Lock()
		h.abandonmentDiagnostics++
		h.mu.Unlock()
	}
	if r.Lvl == log.LvlWarn && r.Msg == "Execution payload unavailable during block production" {
		h.mu.Lock()
		h.unavailableWarnings++
		h.mu.Unlock()
	}
	if r.Lvl > log.LvlWarn {
		return nil
	}
	switch r.Msg {
	case "BlockProduction: Failed to get payload id", "Failed to produce beacon body", "Failed to produce block":
		h.mu.Lock()
		h.failures++
		h.mu.Unlock()
	}
	return nil
}

func (h *blockProductionFailureLogCounter) Enabled(context.Context, log.Lvl) bool { return true }

func (h *blockProductionFailureLogCounter) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failures
}

func (h *blockProductionFailureLogCounter) busyDiagnosticCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.busyDiagnostics
}

func (h *blockProductionFailureLogCounter) abandonmentDiagnosticCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.abandonmentDiagnostics
}

func (h *blockProductionFailureLogCounter) unavailableWarningCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.unavailableWarnings
}

func TestLogBlockProductionFailureWarnsWhenPayloadIsUnavailable(t *testing.T) {
	rec := &blockProductionFailureLogCounter{}
	previousHandler := log.Root().GetHandler()
	log.Root().SetHandler(rec)
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })

	logBlockProductionFailure(errExecutionPayloadUnavailable, 1)

	require.Zero(t, rec.count())
	require.Equal(t, 1, rec.unavailableWarningCount())
}

func TestLogBlockProductionFailureRecordsRequestAbandonmentAtDebug(t *testing.T) {
	rec := &blockProductionFailureLogCounter{}
	previousHandler := log.Root().GetHandler()
	log.Root().SetHandler(rec)
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })

	logBlockProductionFailure(execmodule.RequestAbandonedError(context.Canceled, nil), 1)

	require.Zero(t, rec.count())
	require.Equal(t, 1, rec.abandonmentDiagnosticCount())
}

func TestBlockProductionDoesNotReportRequestAbandonmentAsFailure(t *testing.T) {
	_, blocks, _, _, _, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	h.engine = abandonedBlockProductionEngine{}

	rec := &blockProductionFailureLogCounter{}
	previousHandler := log.Root().GetHandler()
	log.Root().SetHandler(rec)
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })

	targetSlot := blocks[len(blocks)-1].Block.Slot + 1
	url := fmt.Sprintf("/?randao_reveal=%s&skip_randao_verification&graffiti=0x00", hexutil.Encode(make([]byte, 96)))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("slot", fmt.Sprint(targetSlot))
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeContext))

	response, err := h.GetEthV3ValidatorBlock(httptest.NewRecorder(), req)

	require.Nil(t, response)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, execmodule.ErrBusy)
	require.Zero(t, rec.count())
	require.Equal(t, 1, rec.busyDiagnosticCount())
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

func TestExpectedWithdrawalsReadsTheRightSourcePerFork(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	a := &ApiHandler{beaconChainCfg: &cfg}

	capellaState := state.New(&cfg)
	capellaState.SetVersion(clparams.CapellaVersion)

	// Before Gloas the expectation is computed from the head state itself, and the list is present
	// even when empty: the execution layer rejects a nil one after Shanghai.
	withdrawals, err := a.expectedWithdrawals(capellaState, nil, clparams.CapellaVersion, 0)
	require.NoError(t, err)
	require.NotNil(t, withdrawals)
	require.Empty(t, withdrawals)

	gloasState := state.New(&cfg)
	gloasState.SetVersion(clparams.GloasVersion)

	// A Gloas head whose payload was revealed is read from the state copy carrying that payload,
	// not from the head state. Only that copy carries a pending builder withdrawal, so reading the
	// wrong one comes back empty rather than merely equal.
	withParentPayload := state.New(&cfg)
	withParentPayload.SetVersion(clparams.GloasVersion)
	pending := solid.NewDynamicListSSZ[*cltypes.BuilderPendingWithdrawal](int(cfg.MaxWithdrawalsPerPayload))
	pending.Append(&cltypes.BuilderPendingWithdrawal{FeeRecipient: common.Address{0xbb}, Amount: 12, BuilderIndex: 3})
	withParentPayload.SetBuilderPendingWithdrawals(pending)

	withdrawals, err = a.expectedWithdrawals(gloasState, withParentPayload, clparams.GloasVersion, 0)
	require.NoError(t, err)
	require.Equal(t, []*types.Withdrawal{{
		Index:     0,
		Validator: state.ConvertBuilderIndexToValidatorIndex(3),
		Address:   common.Address{0xbb},
		Amount:    12,
	}}, withdrawals)

	// An EMPTY Gloas head uses the expectation the state already cached rather than computing a
	// fresh one, so what it returns is whatever was cached.
	withdrawals, err = a.expectedWithdrawals(gloasState, nil, clparams.GloasVersion, 0)
	require.NoError(t, err)
	require.Empty(t, withdrawals)

	cached := solid.NewDynamicListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload))
	cached.Append(&cltypes.Withdrawal{Index: 7, Validator: 8, Address: common.Address{0xaa}, Amount: 9})
	gloasState.SetPayloadExpectedWithdrawals(cached)
	withdrawals, err = a.expectedWithdrawals(gloasState, nil, clparams.GloasVersion, 0)
	require.NoError(t, err)
	require.Equal(t, []*types.Withdrawal{
		{Index: 7, Validator: 8, Address: common.Address{0xaa}, Amount: 9},
	}, withdrawals)
}

func TestPayloadAttributesOmitFieldsTheChosenVersionCannotCarry(t *testing.T) {
	root := common.Hash{0xaa}
	withdrawals := []*types.Withdrawal{{Index: 1}}
	slotNumber := hexutil.Uint64(64)
	targetGasLimit := hexutil.Uint64(36_000_000)

	for _, tc := range []struct {
		version         clparams.StateVersion
		wantWithdrawals bool
		wantParentRoot  bool
		wantGloasFields bool
	}{
		{clparams.BellatrixVersion, false, false, false},
		{clparams.CapellaVersion, true, false, false},
		{clparams.DenebVersion, true, true, false},
		{clparams.FuluVersion, true, true, false},
		{clparams.GloasVersion, true, true, true},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			attrs := payloadAttributes(tc.version, 1, common.Hash{0xbb}, common.Address{0xcc},
				withdrawals, &root, &slotNumber, &targetGasLimit)

			// A version that does not define a field must not have it populated: V1 carries no
			// withdrawals, V1 and V2 no parent beacon block root, and supplying one is rejected
			// rather than ignored.
			require.Equal(t, tc.wantWithdrawals, attrs.Withdrawals != nil)
			require.Equal(t, tc.wantParentRoot, attrs.ParentBeaconBlockRoot != nil)

			// The values have to arrive, not merely be non-nil: dropping either Gloas field leaves
			// every Gloas proposal rejected with -38003.
			if tc.wantGloasFields {
				require.Equal(t, &slotNumber, attrs.SlotNumber)
				require.Equal(t, &targetGasLimit, attrs.TargetGasLimit)
			} else {
				require.Nil(t, attrs.SlotNumber)
				require.Nil(t, attrs.TargetGasLimit)
			}
			require.Equal(t, hexutil.Uint64(1), attrs.Timestamp)
			require.Equal(t, common.Hash{0xbb}, attrs.PrevRandao)
			require.Equal(t, common.Address{0xcc}, attrs.SuggestedFeeRecipient)
		})
	}
}
