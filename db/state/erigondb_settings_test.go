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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

func TestRefsInCommitmentBranchesAccessor(t *testing.T) {
	t.Parallel()
	tr, fa := true, false
	require.True(t, (&ErigonDBSettings{ReferencesInCommitmentBranches: nil}).RefsInCommitmentBranches())
	require.True(t, (&ErigonDBSettings{ReferencesInCommitmentBranches: &tr}).RefsInCommitmentBranches())
	require.False(t, (&ErigonDBSettings{ReferencesInCommitmentBranches: &fa}).RefsInCommitmentBranches())
}

func TestErigonDBSettingsRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "erigondb.toml")

	fa := false
	require.NoError(t, writeErigonDBSettings(path, &ErigonDBSettings{
		StepSize: 100, StepsInFrozenFile: 8, ReferencesInCommitmentBranches: &fa,
	}))
	got, err := readErigonDBSettings(path)
	require.NoError(t, err)
	require.NotNil(t, got.ReferencesInCommitmentBranches)
	require.False(t, *got.ReferencesInCommitmentBranches)

	tr := true
	require.NoError(t, writeErigonDBSettings(path, &ErigonDBSettings{
		StepSize: 100, StepsInFrozenFile: 8, ReferencesInCommitmentBranches: &tr,
	}))
	got, err = readErigonDBSettings(path)
	require.NoError(t, err)
	require.NotNil(t, got.ReferencesInCommitmentBranches)
	require.True(t, *got.ReferencesInCommitmentBranches)
}

func TestErigonDBSettingsAbsentFieldUnmarshalsNil(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "erigondb.toml")
	require.NoError(t, os.WriteFile(path, []byte("step_size = 100\nsteps_in_frozen_file = 8\n"), 0644))

	got, err := readErigonDBSettings(path)
	require.NoError(t, err)
	require.Nil(t, got.ReferencesInCommitmentBranches)
	require.True(t, got.RefsInCommitmentBranches())
}

func TestResolveErigonDBSettingsExistingFileAbsentFieldNormalizesToTrue(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	path := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)
	content := []byte("step_size = 390625\nsteps_in_frozen_file = 256\n")
	require.NoError(t, os.WriteFile(path, content, 0644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.True(t, settings.RefsInCommitmentBranches())

	// Existing erigondb.toml is synced snapshot metadata and must NOT be rewritten.
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, after)
}

func TestResolveErigonDBSettingsExistingFileExplicitFalseHonored(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	path := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)
	content := []byte("step_size = 390625\nsteps_in_frozen_file = 256\nreferences_in_commitment_branches = false\n")
	require.NoError(t, os.WriteFile(path, content, 0644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.False(t, settings.RefsInCommitmentBranches())

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, after)
}

func TestResolveErigonDBSettingsExistingFileExplicitTrueHonored(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	path := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)
	require.NoError(t, os.WriteFile(path, []byte("references_in_commitment_branches = true\n"), 0644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.True(t, settings.RefsInCommitmentBranches())
}

func TestResolveErigonDBSettingsLegacyWritesTrue(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), []byte(""), 0644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.True(t, settings.RefsInCommitmentBranches())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotNil(t, written.ReferencesInCommitmentBranches)
	require.True(t, *written.ReferencesInCommitmentBranches)
}

func TestResolveErigonDBSettingsFreshNoDownloaderWritesTrue(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.True(t, settings.RefsInCommitmentBranches())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotNil(t, written.ReferencesInCommitmentBranches)
	require.True(t, *written.ReferencesInCommitmentBranches)
}

func TestResolveErigonDBSettingsFreshWithDownloaderDoesNotWrite(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.True(t, settings.RefsInCommitmentBranches())

	_, err = os.Stat(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.True(t, os.IsNotExist(err), "fresh+downloader must leave erigondb.toml for the downloader")
}
