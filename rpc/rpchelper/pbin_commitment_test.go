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

package rpchelper

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// Commitment replay recomputes roots with the hex trie over its own temporary
// aggregator, so it cannot serve a bin datadir.
func TestPBinCommitmentReplayRefusesBin(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	orig := statecfg.ExperimentalBinCommitment
	origParallel := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = orig
		statecfg.ExperimentalParallelCommitment = origParallel
	})
	statecfg.ExperimentalBinCommitment = true
	// erigondb.toml resolution refuses the combination: the bin trie is
	// sequential-only, regardless of a process-wide parallel default.
	statecfg.ExperimentalParallelCommitment = false

	// Fresh dirs: the replay resolves erigondb.toml itself, and a hex toml would
	// be refused there instead of at the SharedDomains this test pins.
	r := NewCommitmentReplay(datadir.New(t.TempDir()), rawdbv3.TxNums, log.New())
	_, err = r.ComputeCustomCommitmentFromStateHistory(t.Context(), tx, 0, nil)
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}
