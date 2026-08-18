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

package chainreader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
)

type contextBoundExecutionModule struct {
	execmodule.ExecutionModule
}

func (contextBoundExecutionModule) UpdateForkChoice(ctx context.Context, _, _, _ common.Hash) (execmodule.ForkChoiceResult, error) {
	<-ctx.Done()
	return execmodule.ForkChoiceResult{}, execmodule.RequestAbandonedError(ctx.Err(), nil)
}

func TestUpdateForkChoiceReportsConfiguredTimeoutAsBusy(t *testing.T) {
	reader := NewChainReaderEth1(nil, contextBoundExecutionModule{}, time.Nanosecond)

	status, _, _, err := reader.UpdateForkChoice(t.Context(), common.Hash{}, common.Hash{}, common.Hash{})

	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBusy, status)
}

func TestUpdateForkChoicePreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader := NewChainReaderEth1(nil, contextBoundExecutionModule{}, time.Hour)

	status, _, _, err := reader.UpdateForkChoice(ctx, common.Hash{}, common.Hash{}, common.Hash{})

	require.Equal(t, execmodule.ExecutionStatus(0), status)
	require.ErrorIs(t, err, execmodule.ErrRequestAbandoned)
	require.ErrorIs(t, err, context.Canceled)
}
