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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

func counterDelta(name string) func() uint64 {
	c := metrics.GetOrCreateCounter(name)
	start := c.GetValueUint64()
	return func() uint64 { return c.GetValueUint64() - start }
}

func TestReadCommitmentRecordsCountsMaskKnowledge(t *testing.T) {
	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	known := counterDelta(`domain_commitment_node_reads{mask="known"}`)
	unknown := counterDelta(`domain_commitment_node_reads{mask="unknown"}`)
	nodeKey := []byte{0x0a, 0x0c}

	_, _, _, err = sd.ReadCommitmentRecords(rwTx, nodeKey, 0, false, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, unknown())
	require.Zero(t, known())

	_, _, _, err = sd.ReadCommitmentRecords(rwTx, nodeKey, 1<<3, true, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, known())
	require.EqualValues(t, 1, unknown(), "a known-mask read must not count as unknown")
}
