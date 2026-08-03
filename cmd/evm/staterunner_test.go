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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestNewStateTestSharedDomainsUsesSelectedCommitment(t *testing.T) {
	originalParallel := statecfg.ExperimentalParallelCommitment
	originalStreaming := statecfg.ExperimentalStreamingCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalParallelCommitment = originalParallel
		statecfg.ExperimentalStreamingCommitment = originalStreaming
	})

	for _, tc := range []struct {
		name     string
		parallel bool
		variant  commitment.TrieVariant
	}{
		{name: "serial", variant: commitment.VariantHexPatriciaTrie},
		{name: "parallel", parallel: true, variant: commitment.VariantParallelHexPatricia},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statecfg.ExperimentalParallelCommitment = tc.parallel
			statecfg.ExperimentalStreamingCommitment = false

			db, tx := temporaltest.NewTestTx(t)
			sd, err := newStateTestSharedDomains(db, tx)
			require.NoError(t, err)
			t.Cleanup(sd.Close)

			require.Equal(t, tc.variant, sd.GetCommitmentCtx().Trie().Variant())
		})
	}
}
