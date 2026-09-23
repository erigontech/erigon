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

package v4

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestProcessPublishesCommitmentMetrics(t *testing.T) {
	keys := metrics.GetOrCreateCounter("commitment_keys_total")
	readBytes := metrics.GetOrCreateCounter("commitment_branch_read_bytes_total")
	writeBytes := metrics.GetOrCreateCounter("commitment_branch_write_bytes_total")
	writes := metrics.GetOrCreateCounter("commitment_branch_writes_total")

	entries := benchEntries("storage", 64)
	grown := benchEntries("storage", 128)
	dir := t.TempDir()
	c := newShardedContext()
	tr := &Trie{}
	tr.ResetContext(c)
	tr.SetTrieContextFactory(c.factory)
	defer tr.Release()

	u := benchUpdatesIn(dir, commitment.ModeCollect, entries)
	_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	beforeKeys := keys.GetValueUint64()
	beforeRead := readBytes.GetValueUint64()
	beforeWrite := writeBytes.GetValueUint64()
	beforeWrites := writes.GetValueUint64()

	u = benchUpdatesIn(dir, commitment.ModeCollect, grown)
	_, err = tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	require.EqualValues(t, len(grown), keys.GetValueUint64()-beforeKeys,
		"commitment_keys_total counts each key of the round once")
	require.Positive(t, readBytes.GetValueUint64()-beforeRead,
		"commitment_branch_read_bytes_total moved")
	require.Positive(t, writeBytes.GetValueUint64()-beforeWrite,
		"commitment_branch_write_bytes_total moved")
	require.Positive(t, writes.GetValueUint64()-beforeWrites,
		"commitment_branch_writes_total moved")
}
