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
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func TestRefsInCommitmentBranchesAccessor(t *testing.T) {
	t.Parallel()
	tr, fa := true, false
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, (&ErigonDBSettings{ReferencesInCommitmentBranches: nil}).RefsInCommitmentBranches())
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

func TestErigonDBSettingsConversionPointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "erigondb.toml")
	blockNum, txNum := uint64(123), uint64(456)
	require.NoError(t, writeErigonDBSettings(path, &ErigonDBSettings{
		StepSize: 100, StepsInFrozenFile: 8,
		ConversionBlockNum: &blockNum, ConversionTxNum: &txNum,
	}))

	got, err := readErigonDBSettings(path)
	require.NoError(t, err)
	blockNum, txNum, ok, err := got.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(123), blockNum)
	require.Equal(t, uint64(456), txNum)
	_, _, ok, err = (&ErigonDBSettings{}).ConversionPoint()
	require.NoError(t, err)
	require.False(t, ok)
	_, _, ok, err = (&ErigonDBSettings{ConversionBlockNum: &blockNum}).ConversionPoint()
	require.ErrorContains(t, err, "requires conversion_block and conversion_txnum")
	require.False(t, ok)
}

func TestResolveErigonDBStepSizeUsesFirstStartChoice(t *testing.T) {
	for _, tc := range []struct {
		name        string
		preverified bool
		want        uint64
	}{
		{name: "fresh", want: config3.DefaultStepSize},
		{name: "legacy", preverified: true, want: config3.LegacyStepSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			if tc.preverified {
				require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), nil, 0o644))
			}
			got, err := ResolveErigonDBStepSize(dirs)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestResolveErigonDBStepSizeReadsExistingSettings(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	refs := false
	require.NoError(t, WriteErigonDBSettings(dirs, &ErigonDBSettings{StepSize: 123, ReferencesInCommitmentBranches: &refs}))
	got, err := ResolveErigonDBStepSize(dirs)
	require.NoError(t, err)
	require.Equal(t, uint64(123), got)
}

func TestEnableCommitmentV3FromFiles(t *testing.T) {
	commitmentflags.Restore(t)
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-1.kv"), nil, 0o644))
	detected, err := EnableCommitmentV3FromFiles(dirs)
	require.NoError(t, err)
	require.True(t, detected)
	require.True(t, statecfg.Schema.CommitmentDomain.CommitmentV3Records)
}

func TestEnableCommitmentV3FromFilesIgnoresV3HistoryIndex(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	files := []string{
		"v2.0-commitment.0-1.kv",
		"v2.0-commitment.0-1.kvi",
		"v3.0-commitment.0-1.ef",
		"v3.0-commitment.0-1.efi",
		"v2.0-commitment.0-1.v",
		"v2.0-commitment.0-1.vi",
	}
	for _, name := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), nil, 0o644))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapHistory, "v3.0-commitment.0-1.ef"), nil, 0o644))
	detected, err := EnableCommitmentV3FromFiles(dirs)
	require.NoError(t, err)
	require.False(t, detected)
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	require.Len(t, entries, len(files))
}

func TestEnableCommitmentV3FromFilesRefusesStraddledCommitmentData(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	for _, name := range []string{"v2.0-commitment.0-1.kv", "v3.0-commitment.1-2.kv"} {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), nil, 0o644))
	}
	_, err := EnableCommitmentV3FromFiles(dirs)
	require.ErrorContains(t, err, "straddle v3.0")
}

func TestEnableCommitmentV3FromFilesIgnoresSupersededLegacyCommitmentData(t *testing.T) {
	commitmentflags.Restore(t)
	dirs := datadir.New(t.TempDir())
	for _, name := range []string{"v2.0-commitment.0-1.kv", "v2.0-commitment.1-2.kv", "v3.0-commitment.0-2.kv"} {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), nil, 0o644))
	}
	detected, err := EnableCommitmentV3FromFiles(dirs)
	require.NoError(t, err)
	require.True(t, detected)
}

func TestEnableCommitmentV3FromFilesIgnoresBinDatadir(t *testing.T) {
	commitmentflags.Restore(t)
	dirs := datadir.New(t.TempDir())
	variant := TrieVariantBin
	require.NoError(t, WriteErigonDBSettings(dirs, &ErigonDBSettings{TrieVariant: &variant}))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-1.kv"), nil, 0o644))
	detected, err := EnableCommitmentV3FromFiles(dirs)
	require.NoError(t, err)
	require.False(t, detected)
}

func TestErigonDBSettingsTrieVariantRoundTrip(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{TrieVariantHex, TrieVariantBin, TrieVariantHexBin} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "erigondb.toml")
			tr := true
			hash := "blake3"
			s := &ErigonDBSettings{TrieVariant: &variant, ReferencesInCommitmentBranches: &tr}
			if variant != TrieVariantHex {
				s.TrieHash = &hash
			}
			require.NoError(t, writeErigonDBSettings(path, s))

			got, err := readErigonDBSettings(path)
			require.NoError(t, err)
			require.Equal(t, variant, got.TrieVariantName())
		})
	}
}

func TestReconcileTrieVariantHexBinEnablesV3Hex(t *testing.T) {
	originalBin := statecfg.ExperimentalBinCommitment
	originalHexBin := statecfg.ExperimentalHexBinCommitment
	originalV3 := statecfg.ExperimentalCommitmentV3
	originalSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = originalBin
		statecfg.ExperimentalHexBinCommitment = originalHexBin
		statecfg.ExperimentalCommitmentV3 = originalV3
		statecfg.Schema = originalSchema
	})
	variant := TrieVariantHexBin
	refs := false
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.Schema = originalSchema
	err := reconcileTrieVariant(&ErigonDBSettings{TrieVariant: &variant, ReferencesInCommitmentBranches: &refs}, log.New())
	require.NoError(t, err)
	require.True(t, statecfg.ExperimentalCommitmentV3)
	require.True(t, statecfg.Schema.CommitmentDomain.CommitmentV3Records)
}

func TestReconcileTrieVariantBinRefusesV3Hex(t *testing.T) {
	originalBin := statecfg.ExperimentalBinCommitment
	originalHexBin := statecfg.ExperimentalHexBinCommitment
	originalV3 := statecfg.ExperimentalCommitmentV3
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = originalBin
		statecfg.ExperimentalHexBinCommitment = originalHexBin
		statecfg.ExperimentalCommitmentV3 = originalV3
	})
	variant := TrieVariantBin
	refs := false
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	err := reconcileTrieVariant(&ErigonDBSettings{TrieVariant: &variant, ReferencesInCommitmentBranches: &refs}, log.New())
	require.ErrorContains(t, err, "v3-hex")
}

func TestErigonDBSettingsFrozenCommitmentRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "erigondb.toml")
	variant := TrieVariantHexBin
	hash := "blake3"
	settings := &ErigonDBSettings{
		StepSize:          100,
		StepsInFrozenFile: 8,
		TrieVariant:       &variant,
		TrieHash:          &hash,
		FrozenAtTxNum: map[string]uint64{
			kv.CommitmentDomain.String():    120,
			kv.CommitmentBinDomain.String(): 240,
		},
	}
	require.NoError(t, writeErigonDBSettings(path, settings))

	got, err := readErigonDBSettings(path)
	require.NoError(t, err)
	require.Equal(t, uint64(120), got.FrozenAtTxNum[kv.CommitmentDomain.String()])
	require.Equal(t, uint64(240), got.FrozenAtTxNum[kv.CommitmentBinDomain.String()])
	frozenAt, frozen := got.FrozenAt(kv.CommitmentDomain)
	require.True(t, frozen)
	require.Equal(t, uint64(120), frozenAt)
}

func TestErigonDBSettingsFrozenCommitmentSurvivesAggregatorOpen(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	variant := TrieVariantHex
	settings := &ErigonDBSettings{
		StepSize:          config3.DefaultStepSize,
		StepsInFrozenFile: config3.DefaultStepsInFrozenFile,
		TrieVariant:       &variant,
		FrozenAtTxNum:     map[string]uint64{kv.CommitmentDomain.String(): 16},
	}
	require.NoError(t, WriteErigonDBSettings(dirs, settings))

	agg := openTestAggForRefs(t, dirs, settings)
	frozenAt, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
	require.True(t, frozen)
	require.Equal(t, uint64(16), frozenAt)
}

func TestErigonDBSettingsAbsentFieldUnmarshalsNil(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "erigondb.toml")
	require.NoError(t, os.WriteFile(path, []byte("step_size = 100\nsteps_in_frozen_file = 8\n"), 0o644))

	got, err := readErigonDBSettings(path)
	require.NoError(t, err)
	require.Nil(t, got.ReferencesInCommitmentBranches)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, got.RefsInCommitmentBranches())
}

func TestResolveErigonDBSettingsExistingFileAbsentFieldResolvesToDefault(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	path := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)
	content := []byte("step_size = 390625\nsteps_in_frozen_file = 256\n")
	require.NoError(t, os.WriteFile(path, content, 0o644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, settings.RefsInCommitmentBranches())

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
	require.NoError(t, os.WriteFile(path, content, 0o644))

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
	require.NoError(t, os.WriteFile(path, []byte("references_in_commitment_branches = true\n"), 0o644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.True(t, settings.RefsInCommitmentBranches())
}

func TestResolveErigonDBSettingsLegacyWritesDefault(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), []byte(""), 0o644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, settings.RefsInCommitmentBranches())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotNil(t, written.ReferencesInCommitmentBranches)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, *written.ReferencesInCommitmentBranches)
}

func TestResolveErigonDBSettingsFreshNoDownloaderWritesDefault(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	require.NotNil(t, settings.ReferencesInCommitmentBranches)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, settings.RefsInCommitmentBranches())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotNil(t, written.ReferencesInCommitmentBranches)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, *written.ReferencesInCommitmentBranches)
}

func TestResolveErigonDBSettingsFreshWithDownloaderDoesNotWrite(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, settings.RefsInCommitmentBranches())

	_, err = os.Stat(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.True(t, os.IsNotExist(err), "fresh+downloader must leave erigondb.toml for the downloader")
}

func TestResolveErigonDBSettingsWithRefsDefaultFreshNoDownloader(t *testing.T) {
	t.Parallel()
	tr, fa := true, false
	for _, tc := range []struct {
		name           string
		refsFirstStart *bool
		want           bool
	}{
		{"plain_writes_false", &fa, false},
		{"referenced_writes_true", &tr, true},
		{"unset_uses_config_default", nil, config3.DefaultReferencesInCommitmentBranches},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dirs := datadir.New(t.TempDir())
			settings, err := ResolveErigonDBSettingsWithRefsDefault(dirs, log.New(), true, tc.refsFirstStart)
			require.NoError(t, err)
			require.Equal(t, tc.want, settings.RefsInCommitmentBranches())

			written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
			require.NoError(t, err)
			require.NotNil(t, written.ReferencesInCommitmentBranches)
			require.Equal(t, tc.want, *written.ReferencesInCommitmentBranches)
		})
	}
}

func TestResolveErigonDBSettingsWithRefsDefaultLegacyWritesChosen(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), []byte(""), 0o644))

	fa := false
	settings, err := ResolveErigonDBSettingsWithRefsDefault(dirs, log.New(), false, &fa)
	require.NoError(t, err)
	require.False(t, settings.RefsInCommitmentBranches())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotNil(t, written.ReferencesInCommitmentBranches)
	require.False(t, *written.ReferencesInCommitmentBranches)
}

func TestResolveErigonDBSettingsWithRefsDefaultExistingFileIgnoresFlag(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	path := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)
	content := []byte("step_size = 390625\nsteps_in_frozen_file = 256\nreferences_in_commitment_branches = true\n")
	require.NoError(t, os.WriteFile(path, content, 0o644))

	fa := false
	settings, err := ResolveErigonDBSettingsWithRefsDefault(dirs, log.New(), false, &fa)
	require.NoError(t, err)
	require.True(t, settings.RefsInCommitmentBranches(), "existing file wins over the first-start flag")

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, after)
}

func TestResolveErigonDBSettingsWithRefsDefaultFreshWithDownloaderDoesNotWrite(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())

	fa := false
	settings, err := ResolveErigonDBSettingsWithRefsDefault(dirs, log.New(), false, &fa)
	require.NoError(t, err)
	require.False(t, settings.RefsInCommitmentBranches(), "in-memory value reflects the flag until the downloader delivers the file")

	_, err = os.Stat(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.True(t, os.IsNotExist(err), "fresh+downloader must leave erigondb.toml for the downloader")
}
