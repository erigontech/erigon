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

package integrity

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func withBinCommitment(t *testing.T, on bool) {
	t.Helper()
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = on
}

// The history checks recompute roots with the hex trie: on a bin datadir they must
// refuse, not report a mismatch against correct bin records.
func TestPBinCommitmentHistChecksRefuseBin(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	withBinCommitment(t, true)

	err := CheckCommitmentHistAtBlk(t.Context(), db, nil, 1, log.LvlInfo, log.New())
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)

	sc, err := NewSamplerCfg(1, 1.0)
	require.NoError(t, err)
	err = CheckCommitmentHistAtBlkRange(t.Context(), sc, db, nil, 0, 1, log.New())
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}
