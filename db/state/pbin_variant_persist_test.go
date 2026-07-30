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
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

// The tests below mutate process-wide statecfg flags, so none of them may run
// in parallel; save/restore keeps the rest of the package unaffected.
func pbinWithVariantFlags(t *testing.T, bin, streaming, parallel bool) {
	t.Helper()
	origBin := statecfg.ExperimentalBinCommitment
	origStream := statecfg.ExperimentalStreamingCommitment
	origPar := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = origBin
		statecfg.ExperimentalStreamingCommitment = origStream
		statecfg.ExperimentalParallelCommitment = origPar
	})
	statecfg.ExperimentalBinCommitment = bin
	statecfg.ExperimentalStreamingCommitment = streaming
	statecfg.ExperimentalParallelCommitment = parallel
}

func pbinWriteToml(t *testing.T, dirs datadir.Dirs, content string) string {
	t.Helper()
	path := filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE)
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}

func TestPBinVariantFirstStartPersistsBin(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	require.Equal(t, TrieVariantBin, settings.TrieVariantName())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotNil(t, written.TrieVariant)
	require.Equal(t, TrieVariantBin, *written.TrieVariant)
}

func TestPBinVariantHexFirstStartWritesNoVariantKey(t *testing.T) {
	pbinWithVariantFlags(t, false, false, false)
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	require.Equal(t, TrieVariantHex, settings.TrieVariantName())

	raw, err := os.ReadFile(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "trie_variant")
}

func TestPBinVariantFlaglessRestartStaysBin(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)
	dirs := datadir.New(t.TempDir())
	_, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)

	// Flagless restart: the persisted trie_variant wins over the CLI default
	// and is adopted process-wide.
	statecfg.ExperimentalBinCommitment = false
	settings, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	require.Equal(t, TrieVariantBin, settings.TrieVariantName())
	require.True(t, statecfg.ExperimentalBinCommitment)
	require.Equal(t, commitment.VariantBinPatriciaTrie, execctx.PickTrieVariant())
}

func TestPBinVariantHexDatadirRefusesBinFlag(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)

	for name, content := range map[string]string{
		"absent_field": "step_size = 100\nsteps_in_frozen_file = 8\n",
		"explicit_hex": "step_size = 100\nsteps_in_frozen_file = 8\ntrie_variant = \"hex\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			pbinWriteToml(t, dirs, content)
			_, err := ResolveErigonDBSettings(dirs, log.New(), false)
			require.Error(t, err)
		})
	}
}

func TestPBinVariantBinDatadirRefusesStreamingAndParallel(t *testing.T) {
	const binToml = "step_size = 100\nsteps_in_frozen_file = 8\ntrie_variant = \"bin\"\n"

	t.Run("streaming", func(t *testing.T) {
		pbinWithVariantFlags(t, false, true, false)
		dirs := datadir.New(t.TempDir())
		pbinWriteToml(t, dirs, binToml)
		_, err := ResolveErigonDBSettings(dirs, log.New(), false)
		require.Error(t, err)
	})
	t.Run("parallel", func(t *testing.T) {
		pbinWithVariantFlags(t, false, false, true)
		dirs := datadir.New(t.TempDir())
		pbinWriteToml(t, dirs, binToml)
		_, err := ResolveErigonDBSettings(dirs, log.New(), false)
		require.Error(t, err)
	})
}

func TestPBinVariantRefusesReferences(t *testing.T) {
	t.Run("persisted", func(t *testing.T) {
		pbinWithVariantFlags(t, false, false, false)
		dirs := datadir.New(t.TempDir())
		pbinWriteToml(t, dirs, "step_size = 100\nsteps_in_frozen_file = 8\nreferences_in_commitment_branches = true\ntrie_variant = \"bin\"\n")
		_, err := ResolveErigonDBSettings(dirs, log.New(), false)
		require.Error(t, err)
	})
	t.Run("first_start", func(t *testing.T) {
		pbinWithVariantFlags(t, true, false, false)
		dirs := datadir.New(t.TempDir())
		refs := true
		_, err := ResolveErigonDBSettingsWithRefsDefault(dirs, log.New(), true, &refs)
		require.Error(t, err)
	})
}

func TestPBinVariantLegacyDatadirRefusesBin(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), []byte(""), 0644))

	_, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.Error(t, err)
}

func TestPBinVariantUnknownVariantRefused(t *testing.T) {
	pbinWithVariantFlags(t, false, false, false)
	dirs := datadir.New(t.TempDir())
	pbinWriteToml(t, dirs, "step_size = 100\nsteps_in_frozen_file = 8\ntrie_variant = \"verkle\"\n")

	_, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.Error(t, err)
}

func TestPBinVariantFreshWithDownloaderPersistsBin(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)
	dirs := datadir.New(t.TempDir())

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.Equal(t, TrieVariantBin, settings.TrieVariantName())

	written, err := readErigonDBSettings(filepath.Join(dirs.Snap, ERIGONDB_SETTINGS_FILE))
	require.NoError(t, err, "a bin datadir must persist its variant at first start, downloader or not")
	require.Equal(t, TrieVariantBin, written.TrieVariantName())
	require.Equal(t, uint64(config3.DefaultStepSize), written.StepSize)
}

// A chain with no published snapshot hashes gets an empty preverified.toml
// committed by the snapshots stage. Without a persisted variant that reads as a
// legacy datadir at the next resolve, and the bin run is refused on its own
// fresh datadir.
func TestPBinVariantSurvivesEmptyPreverifiedFromSnapshotsStage(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)
	dirs := datadir.New(t.TempDir())

	_, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), []byte(""), 0644))

	settings, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	require.Equal(t, TrieVariantBin, settings.TrieVariantName())
}

func TestPBinVariantFreshWithDownloaderRefusesDeliveredHexToml(t *testing.T) {
	pbinWithVariantFlags(t, true, false, false)
	dirs := datadir.New(t.TempDir())

	_, err := ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)

	// A downloader-delivered hex toml overwrites the persisted bin one; the next
	// resolve must refuse rather than silently adopt hex.
	pbinWriteToml(t, dirs, "step_size = 100\nsteps_in_frozen_file = 8\n")
	_, err = ResolveErigonDBSettings(dirs, log.New(), false)
	require.Error(t, err)
}
