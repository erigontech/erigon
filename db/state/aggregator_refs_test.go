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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// saveGlobalCommitmentRefs records the current global schema commitment-refs flag and
// registers a cleanup that restores it, so a test may mutate global state safely.
func saveGlobalCommitmentRefs(t *testing.T) {
	t.Helper()
	restore := statecfg.Schema.CommitmentDomain.ReferencesInCommitmentBranches
	t.Cleanup(func() { statecfg.Schema.CommitmentDomain.ReferencesInCommitmentBranches = restore })
}

func writeRefsToml(t *testing.T, dirs datadir.Dirs, refs bool) {
	t.Helper()
	content := fmt.Appendf(nil, "step_size = %d\nsteps_in_frozen_file = %d\nreferences_in_commitment_branches = %v\n",
		config3.DefaultStepSize, config3.DefaultStepsInFrozenFile, refs)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE), content, 0644))
}

func openTestAggForRefs(t *testing.T, dirs datadir.Dirs, settings *ErigonDBSettings) *Aggregator {
	t.Helper()
	logger := log.New()
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).Logger(logger).WithErigonDBSettings(settings).MustOpen(t.Context(), db)
	t.Cleanup(agg.Close)
	return agg
}

// ReloadErigonDBSettings is the primary runtime path; a toml that says false must
// land on both the global schema and the live commitment domain.
func TestReloadErigonDBSettingsAppliesCommitmentRefsFlag(t *testing.T) {
	saveGlobalCommitmentRefs(t)

	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, false)

	logger := log.New()
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
	t.Cleanup(agg.Close)

	require.NoError(t, agg.ReloadErigonDBSettings(true))

	require.False(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches, "live commitment domain reflects toml false")
	require.False(t, statecfg.Schema.CommitmentDomain.ReferencesInCommitmentBranches, "global schema reflects toml false")
}

// The builder path (WithErigonDBSettings → Open) is used by every standalone entry point;
// the resolved flag must reach the live commitment domain and the global schema there too.
func TestOpenAppliesCommitmentRefsFlagFalse(t *testing.T) {
	saveGlobalCommitmentRefs(t)

	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, false)
	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.False(t, settings.RefsInCommitmentBranches())

	agg := openTestAggForRefs(t, dirs, settings)

	require.False(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches, "live commitment domain reflects resolved false")
	require.False(t, statecfg.Schema.CommitmentDomain.ReferencesInCommitmentBranches, "global schema reflects resolved false")
}

func TestOpenAppliesCommitmentRefsFlagTrue(t *testing.T) {
	saveGlobalCommitmentRefs(t)

	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, true)
	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.True(t, settings.RefsInCommitmentBranches())

	agg := openTestAggForRefs(t, dirs, settings)

	require.True(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches, "live commitment domain reflects resolved true")
	require.True(t, statecfg.Schema.CommitmentDomain.ReferencesInCommitmentBranches, "global schema reflects resolved true")
}
