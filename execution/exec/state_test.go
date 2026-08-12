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
