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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

// A reorg unwind restores domain values from the diffset, so a domain missing
// from GetDiffset is silently never rolled back.
func TestGetDiffsetCoversAllDomains(t *testing.T) {
	cs := &changeset.StateChangeSet{}
	for d := range kv.DomainLen {
		cs.Diffs[d].DomainUpdate([]byte{byte(d)}, 0, []byte("prev"))
	}

	sd := &TemporalMemBatch{}
	blockHash := common.Hash{1}
	sd.SavePastChangesetAccumulator(blockHash, 1, cs)

	diffs, ok, err := sd.GetDiffset(nil, blockHash, 1)
	require.NoError(t, err)
	require.True(t, ok)
	for d := range kv.DomainLen {
		require.NotEmpty(t, diffs[d], "domain %s missing from GetDiffset", d)
	}
}
