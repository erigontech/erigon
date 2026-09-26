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

package v3

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

func TestMetrics(t *testing.T) {
	names := []string{"commitment_keys_total", "commitment_branch_read_bytes_total", "commitment_branch_write_bytes_total", "commitment_branch_writes_total"}
	read := func() []uint64 {
		values := make([]uint64, len(names))
		for i, name := range names {
			values[i] = metrics.GetOrCreateCounter(name).GetValueUint64()
		}
		return values
	}

	t.Run("process", func(t *testing.T) {
		c := newShardedContext()
		cfg := v3Config{trie: &Trie{}}
		defer cfg.trie.Release()
		runV3(t, c, cfg, benchEntries("storage", 64))
		grown := benchEntries("storage", 128)
		before := read()
		runV3(t, c, cfg, grown)
		after := read()
		require.EqualValues(t, len(grown), after[0]-before[0], "commitment_keys_total counts each key of the round once")
		for i := 1; i < len(names); i++ {
			require.Positive(t, after[i]-before[i], "%s moved", names[i])
		}
	})

	t.Run("deferred_bills_branch_writes", func(t *testing.T) {
		before := read()
		runV3(t, newShardedContext(), v3Config{deferred: true}, benchEntries("storage", 64))
		after := read()
		for i := 2; i < len(names); i++ {
			require.Positive(t, after[i]-before[i], "deferred rounds still bill %s", names[i])
		}
	})
}
