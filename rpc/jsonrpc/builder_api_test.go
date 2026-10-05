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

package jsonrpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/rpc"
)

func dialBuilderStatus(t *testing.T, status *executionbuilder.EmbeddedBuilderStatus) *rpc.Client {
	t.Helper()
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	t.Cleanup(server.Stop)
	require.NoError(t, server.RegisterName("builder", BuilderAPI(NewBuilderAPI(status))))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(client.Close)
	return client
}

func TestBuilderStatusReportsRuntime(t *testing.T) {
	status := executionbuilder.NewEmbeddedBuilderStatus(true)
	status.MarkRunning()
	status.RecordAttempt(42)
	status.RecordBid(41, 123)
	status.RecordOutcome(42, executionbuilder.BuilderOutcomeExecutionBusy)

	var got BuilderStatus
	require.NoError(t, dialBuilderStatus(t, status).CallContext(t.Context(), &got, "builder_status"))
	require.True(t, got.Enabled)
	require.Equal(t, "running", got.Phase)
	require.Empty(t, got.Reason)
	require.Equal(t, hexutil.Uint64(42), got.LastAttemptSlot)
	require.Equal(t, hexutil.Uint64(41), got.LastBidSlot)
	require.Equal(t, hexutil.Uint64(123), got.LastBidValueGwei)
	require.Equal(t, hexutil.Uint64(42), got.LastOutcomeSlot)
	require.Equal(t, executionbuilder.BuilderOutcomeExecutionBusy, got.LastOutcome)
}

func TestBuilderStatusReportsDisabledReason(t *testing.T) {
	status := executionbuilder.NewEmbeddedBuilderStatus(true)
	status.MarkDisabled(executionbuilder.BuilderDisabledPendingPayloadStore)
	client := dialBuilderStatus(t, status)

	var got BuilderStatus
	require.NoError(t, client.CallContext(t.Context(), &got, "builder_status"))
	require.True(t, got.Enabled)
	require.Equal(t, "disabled", got.Phase)
	require.Equal(t, executionbuilder.BuilderDisabledPendingPayloadStore, got.Reason)

	var raw json.RawMessage
	require.NoError(t, client.CallContext(t.Context(), &raw, "builder_status"))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	require.JSONEq(t, `"0x0"`, string(fields["lastOutcomeSlot"]))
	require.JSONEq(t, `"0x0"`, string(fields["availableCollateralGwei"]))
	require.NotContains(t, fields, "lastOutcome")

	status.RecordAvailableCollateral(20_000_000_000)
	require.NoError(t, client.CallContext(t.Context(), &raw, "builder_status"))
	require.NoError(t, json.Unmarshal(raw, &fields))
	require.JSONEq(t, `"0x4a817c800"`, string(fields["availableCollateralGwei"]))
}
