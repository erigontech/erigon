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

package cltypes_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// Gloas carries the execution requests as progressive lists, which are
// semantically unbounded, so a payload holding more than the Electra
// per-payload maximum must still decode.
func TestExecutionRequestsGloasDecodesAboveElectraMaxima(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig

	for _, tc := range []struct {
		name  string
		count uint64
		fill  func(*cltypes.ExecutionRequests, int)
		got   func(*cltypes.ExecutionRequests) int
	}{
		{
			name:  "deposits",
			count: cfg.MaxDepositRequestsPerPayload,
			fill:  func(e *cltypes.ExecutionRequests, n int) { appendN(n, e.Deposits, &solid.DepositRequest{}) },
			got:   func(e *cltypes.ExecutionRequests) int { return e.Deposits.Len() },
		},
		{
			name:  "withdrawals",
			count: cfg.MaxWithdrawalRequestsPerPayload,
			fill:  func(e *cltypes.ExecutionRequests, n int) { appendN(n, e.Withdrawals, &solid.WithdrawalRequest{}) },
			got:   func(e *cltypes.ExecutionRequests) int { return e.Withdrawals.Len() },
		},
		{
			name:  "consolidations",
			count: cfg.MaxConsolidationRequestsPerPayload,
			fill:  func(e *cltypes.ExecutionRequests, n int) { appendN(n, e.Consolidations, &solid.ConsolidationRequest{}) },
			got:   func(e *cltypes.ExecutionRequests) int { return e.Consolidations.Len() },
		},
		{
			name:  "builder deposits",
			count: cfg.MaxBuilderDepositRequestsPerPayload,
			fill: func(e *cltypes.ExecutionRequests, n int) {
				appendN(n, e.BuilderDeposits, &solid.BuilderDepositRequest{})
			},
			got: func(e *cltypes.ExecutionRequests) int { return e.BuilderDeposits.Len() },
		},
		{
			name:  "builder exits",
			count: cfg.MaxBuilderExitRequestsPerPayload,
			fill:  func(e *cltypes.ExecutionRequests, n int) { appendN(n, e.BuilderExits, &solid.BuilderExitRequest{}) },
			got:   func(e *cltypes.ExecutionRequests) int { return e.BuilderExits.Len() },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The old guard was progressiveDecodeLimit(count), which doubles the
			// configured limit but floors at 16. Exceed whichever applies.
			want := max(int(tc.count)*2, 16) + 1
			requests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
			tc.fill(requests, want)
			encoded, err := requests.EncodeSSZ(nil)
			require.NoError(t, err)

			decoded := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
			require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
			require.Equal(t, want, tc.got(decoded))
		})
	}
}

func appendN[T solid.EncodableHashableSSZ](n int, list *solid.ListSSZ[T], value T) {
	for range n {
		list.Append(value)
	}
}

// The guard is a resource bound, so it must sit at one req/resp chunk, not the
// two the progressive constructors would give it unadjusted.
func TestExecutionRequestsGloasRejectsAboveOneChunk(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	perChunk := int(clparams.MaxChunkSize) / solid.SizeDepositRequest

	for _, tc := range []struct {
		name    string
		count   int
		wantErr bool
	}{
		{name: "at the chunk bound", count: perChunk},
		{name: "above the chunk bound", count: perChunk + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
			appendN(tc.count, requests.Deposits, &solid.DepositRequest{})
			encoded, err := requests.EncodeSSZ(nil)
			require.NoError(t, err)

			decoded := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
			err = decoded.DecodeSSZ(encoded, int(clparams.GloasVersion))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.count, decoded.Deposits.Len())
		})
	}
}

// DecodeSSZ keeps whatever lists the object already holds, so a JSON-built one
// must carry the same guards as a freshly constructed one.
func TestExecutionRequestsGloasJSONKeepsProgressiveLimits(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	want := int(cfg.MaxDepositRequestsPerPayload)*2 + 1

	fromJSON := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	require.NoError(t, fromJSON.UnmarshalJSON([]byte(`{"deposits":[]}`)))

	requests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	appendN(want, requests.Deposits, &solid.DepositRequest{})
	encoded, err := requests.EncodeSSZ(nil)
	require.NoError(t, err)

	require.NoError(t, fromJSON.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, want, fromJSON.Deposits.Len())
}
