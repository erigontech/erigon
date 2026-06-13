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
	"github.com/erigontech/erigon/db/version"
)

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
// land on the live commitment domain.
func TestReloadErigonDBSettingsAppliesCommitmentRefsFlag(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, false)

	logger := log.New()
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(db.Close)
	agg := NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
	t.Cleanup(agg.Close)

	require.NoError(t, agg.ReloadErigonDBSettings(true))

	require.False(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches, "live commitment domain reflects toml false")
}

// The builder path (WithErigonDBSettings → Open) is used by every standalone entry point;
// the resolved flag must reach the live commitment domain.
func TestOpenAppliesCommitmentRefsFlagFalse(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, false)
	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.False(t, settings.RefsInCommitmentBranches())

	agg := openTestAggForRefs(t, dirs, settings)

	require.False(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches, "live commitment domain reflects resolved false")
}

func TestOpenAppliesCommitmentRefsFlagTrue(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, true)
	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.True(t, settings.RefsInCommitmentBranches())

	agg := openTestAggForRefs(t, dirs, settings)

	require.True(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches, "live commitment domain reflects resolved true")
}

// The resolved erigondb.toml regime must bind to the produced commitment file version:
// an in-place upgrade (toml without the field -> default true) keeps writing v2.0
// referenced files; a downloaded plain set (references=false) writes v2.1 plain files.
func TestResolvedRefsFlagBindsCommitmentWriteVersion(t *testing.T) {
	t.Run("in-place upgrade without field writes v2.0 referenced", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		content := fmt.Appendf(nil, "step_size = %d\nsteps_in_frozen_file = %d\n",
			config3.DefaultStepSize, config3.DefaultStepsInFrozenFile)
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE), content, 0644))

		settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
		require.NoError(t, err)
		require.True(t, settings.RefsInCommitmentBranches())

		agg := openTestAggForRefs(t, dirs, settings)
		commit := agg.d[kv.CommitmentDomain]
		require.True(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches)
		require.Equal(t, version.V2_0, commit.kvWriteVersion())
		require.Contains(t, commit.kvNewFilePath(0, 1), "v2.0-commitment.0-1.kv")
	})

	t.Run("downloaded plain set writes v2.1 plain", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		writeRefsToml(t, dirs, false)

		settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
		require.NoError(t, err)
		require.False(t, settings.RefsInCommitmentBranches())

		agg := openTestAggForRefs(t, dirs, settings)
		commit := agg.d[kv.CommitmentDomain]
		require.False(t, agg.Cfg(kv.CommitmentDomain).ReferencesInCommitmentBranches)
		require.Equal(t, version.V2_1, commit.kvWriteVersion())
		require.Contains(t, commit.kvNewFilePath(0, 1), "v2.1-commitment.0-1.kv")
	})
}
