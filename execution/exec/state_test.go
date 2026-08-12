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

package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/execution/chain"
)

func TestNewWorkersPoolForegroundReturnsWait(t *testing.T) {
	in := NewQueueWithRetry(1)
	t.Cleanup(in.Release)

	_, _, _, clear, wait, err := NewWorkersPool(
		context.Background(), WorkerFaults{}, nil, false, nil,
		nil, nil, nil, in, nil, chain.AllProtocolChanges, nil,
		nil, 1, NewWorkerMetrics(), datadir.Dirs{}, log.New(),
	)
	require.NoError(t, err)
	t.Cleanup(clear)
	require.NotNil(t, wait)
	require.NoError(t, wait())
}

// The background pool must keep every worker's failure, mirroring the
// executor-group guarantee: a concurrent second failure is diagnostic
// evidence, not noise to drop on the first-error slot.
func TestNewWorkersPoolPreservesEveryWorkerFailure(t *testing.T) {
	in := NewQueueWithRetry(1)
	t.Cleanup(in.Release)

	boom := errors.New("worker start fault")
	var n atomic.Int64
	faults := WorkerFaults{RunStart: func() error {
		return fmt.Errorf("worker %d: %w", n.Add(1), boom)
	}}

	_, _, _, clear, wait, err := NewWorkersPool(
		context.Background(), faults, nil, true, nil,
		nil, nil, nil, in, nil, chain.AllProtocolChanges, nil,
		nil, 2, NewWorkerMetrics(), datadir.Dirs{}, log.New(),
	)
	require.NoError(t, err)
	t.Cleanup(clear)

	got := wait()
	require.ErrorIs(t, got, boom)
	require.Equal(t, 2, strings.Count(got.Error(), boom.Error()),
		"both workers' failures must survive the join")
}
