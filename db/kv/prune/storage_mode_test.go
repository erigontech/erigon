// Copyright 2024 The Erigon Authors
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

package prune

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		prune, err := Get(tx)
		assert.NoError(t, err)
		assert.Equal(t, DefaultMode, prune)
	})

	t.Run("setIfNotExist", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		prune, err := Get(tx)
		assert.NoError(t, err)
		assert.Equal(t, DefaultMode, prune)

		err = setIfNotExist(tx, FullMode)
		assert.NoError(t, err)

		prune, err = Get(tx)
		assert.NoError(t, err)
		assert.Equal(t, FullMode, prune)
	})
}

func TestModeString_LegacyShapes(t *testing.T) {
	// Pre-EIP-8252 full mode persisted as {Blocks: DefaultBlocksPruneMode,
	// History: Distance(100_000)}. Before the recognition logic, this rendered
	// as "archive --prune.distance=100000 --prune.distance.blocks=18446744073709551615"
	// — accurate but misleading. It now renders as "full(legacy)".
	legacyFull := Mode{Initialised: true, History: Distance(100_000), Blocks: DefaultBlocksPruneMode}
	assert.Equal(t, "full(legacy) --prune.distance=100000", legacyFull.String())

	// Pre-EIP-8252 full with the current default history distance (262_144) —
	// label as plain "full(legacy)" with no override clause.
	legacyFullCurrentHistory := Mode{Initialised: true, History: Distance(262_144), Blocks: DefaultBlocksPruneMode}
	assert.Equal(t, "full(legacy)", legacyFullCurrentHistory.String())

	// Pre-EIP-8252 blocks mode persisted as {Blocks: KeepAllBlocksPruneMode,
	// History: Distance(100_000)}. Render as "blocks --prune.distance=100000".
	legacyBlocks := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode}
	assert.Equal(t, "blocks --prune.distance=100000", legacyBlocks.String())

	// Archive with explicit distance overrides (archive_override) stays on the
	// "archive" base — historical contract preserved.
	archiveOverride := Mode{Initialised: true, History: Distance(400500), Blocks: Distance(100500)}
	assert.Equal(t, "archive --prune.distance=400500 --prune.distance.blocks=100500", archiveOverride.String())
}

func TestParseCLIMode(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		mode, err := FromCli(fullModeStr, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, FullMode, mode)

		assert.Equal(t, "full", mode.String())
	})
	t.Run("archive", func(t *testing.T) {
		mode, err := FromCli(archiveModeStr, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, ArchiveMode, mode)
		assert.Equal(t, archiveModeStr, mode.String())
	})
	t.Run("archive_override", func(t *testing.T) {
		// Operator typed `--prune.mode=archive --prune.distance=400500 --prune.distance.blocks=100500`.
		// FromCli starts with ArchiveMode and overrides both fields to finite
		// Distances. The resulting mode is shape-wise non-archive (finite
		// distances cause distance-based pruning), but Mode.String() preserves
		// the operator's chosen base — see the function docstring.
		exp := ArchiveMode
		exp.Blocks = Distance(100500)
		exp.History = Distance(400500)

		mode, err := FromCli(archiveModeStr, exp.History.toValue(), exp.Blocks.toValue())
		assert.NoError(t, err)
		assert.Equal(t, exp, mode)
		assert.Equal(t, "archive --prune.distance=400500 --prune.distance.blocks=100500", mode.String())
	})
	t.Run("minimal", func(t *testing.T) {
		mode, err := FromCli(minimalModeStr, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, MinimalMode, mode)
		assert.Equal(t, minimalModeStr, mode.String())
	})
	t.Run("garbage", func(t *testing.T) {
		_, err := FromCli("garb", 1, 2)
		assert.ErrorIs(t, err, ErrUnknownPruneMode)
	})
	t.Run("empty", func(t *testing.T) {
		mode, err := FromCli("", 0, 0)
		assert.NoError(t, err)

		assert.Equal(t, DefaultMode, mode)
		assert.Equal(t, "archive", mode.String())
	})
}

var distanceTests = []struct {
	stageHead uint64
	pruneTo   uint64
	expected  uint64
}{
	{3_000_000, 1, 2_999_999},
	{3_000_000, 4_000_000, 0},
	{3_000_000, math.MaxUint64, 0},
	{3_000_000, 1_000_000, 2_000_000},
}

func TestDistancePruneTo(t *testing.T) {
	for _, tt := range distanceTests {
		t.Run(strconv.FormatUint(tt.pruneTo, 10), func(t *testing.T) {
			stageHead := tt.stageHead
			d := Distance(tt.pruneTo)
			pruneTo := d.PruneTo(stageHead)

			if pruneTo != tt.expected {
				t.Errorf("got %d, want %d", pruneTo, tt.expected)
			}
		})
	}
}

func TestIsRetentionWindowChange(t *testing.T) {
	cases := []struct {
		name      string
		persisted Mode
		requested Mode
		want      bool
	}{
		{
			name:      "identical minimal — no change",
			persisted: MinimalMode,
			requested: MinimalMode,
			want:      false,
		},
		{
			name:      "identical archive — no change",
			persisted: ArchiveMode,
			requested: ArchiveMode,
			want:      false,
		},
		{
			name:      "history widened, blocks unchanged finite",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000)},
			requested: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(100_000)},
			want:      true,
		},
		{
			name:      "blocks narrowed, history unchanged finite",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(262_144)},
			requested: Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000)},
			want:      true,
		},
		{
			name:      "both fields widened, both finite",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000)},
			requested: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)},
			want:      true,
		},
		{
			name:      "history widened, blocks unchanged sentinel (blocks mode upgrade)",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode},
			requested: Mode{Initialised: true, History: Distance(262_144), Blocks: KeepAllBlocksPruneMode},
			want:      true,
		},
		{
			name:      "blocks DefaultBlocksPruneMode→finite (full mode EIP-8252 upgrade)",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: DefaultBlocksPruneMode},
			requested: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)},
			want:      true,
		},
		{
			name:      "blocks finite→DefaultBlocksPruneMode (revert after auto-upgrade)",
			persisted: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)},
			requested: Mode{Initialised: true, History: Distance(100_000), Blocks: DefaultBlocksPruneMode},
			want:      true,
		},
		{
			name:      "blocks KeepAllBlocksPruneMode→finite (archive/blocks → finite, rejected)",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode},
			requested: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)},
			want:      false,
		},
		{
			name:      "blocks finite→KeepAllBlocksPruneMode (archive switch, rejected)",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000)},
			requested: Mode{Initialised: true, History: Distance(math.MaxUint64), Blocks: KeepAllBlocksPruneMode},
			want:      false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isRetentionWindowChange(tc.persisted, tc.requested))
		})
	}
}

// initStoredMode writes the given mode into the DB unconditionally so tests
// can simulate a node that started under a different binary.
func initStoredMode(t *testing.T, tx kv.RwTx, m Mode) {
	t.Helper()
	require.NoError(t, overwriteStoredMode(tx, m))
}

func TestEnsureNotChanged_PersistedEqualsRequested(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	initStoredMode(t, tx, MinimalMode)

	got, err := EnsureNotChanged(tx, MinimalMode)
	require.NoError(t, err)
	assert.Equal(t, MinimalMode, got)
}

func TestEnsureNotChanged_LegacyMinimalNoOp(t *testing.T) {
	// MinimalMode now references MinimalPruneDistance (still 100_000), so a node
	// initialized before the rescope has identical persisted state and starts
	// without warning or DB rewrite.
	_, tx := memdb.NewTestTx(t)
	legacy := Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000)}
	initStoredMode(t, tx, legacy)

	got, err := EnsureNotChanged(tx, MinimalMode)
	require.NoError(t, err)
	assert.Equal(t, MinimalMode, got)
	assert.Equal(t, legacy, got, "legacy minimal values must equal current MinimalMode")
}

func TestEnsureNotChanged_BlocksHistoryBumpRewritesDB(t *testing.T) {
	// Pre-rescope blocks mode: {KeepAllBlocksPruneMode, Distance(100_000)}.
	// The new binary's BlocksMode has History=Distance(262_144). The compat
	// shim should accept the finite→finite History change, return the new mode,
	// and persist it so the next restart sees no mismatch.
	_, tx := memdb.NewTestTx(t)
	legacyBlocks := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode}
	initStoredMode(t, tx, legacyBlocks)

	got, err := EnsureNotChanged(tx, BlocksMode)
	require.NoError(t, err)
	assert.Equal(t, BlocksMode, got)

	persisted, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, BlocksMode, persisted, "shim must rewrite the persisted value")
}

func TestEnsureNotChanged_FullSentinelToFiniteAccepted(t *testing.T) {
	// Pre-rescope full mode: {DefaultBlocksPruneMode (sentinel), Distance(100_000)}.
	// New FullMode has Blocks=Distance(262_144). The shim treats this specific
	// one-way DefaultBlocksPruneMode→finite transition on Blocks as a
	// retention-window change so existing full nodes upgrade without operator
	// intervention. (Frozen .seg files won't actually be deleted until #21306
	// lands; the config-level transition is still recorded.)
	_, tx := memdb.NewTestTx(t)
	legacyFull := Mode{Initialised: true, History: Distance(100_000), Blocks: DefaultBlocksPruneMode}
	initStoredMode(t, tx, legacyFull)

	got, err := EnsureNotChanged(tx, FullMode)
	require.NoError(t, err)
	assert.Equal(t, FullMode, got)

	persisted, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, FullMode, persisted, "shim must rewrite the persisted value")
}

func TestEnsureNotChanged_BlocksFiniteToDefaultAccepted(t *testing.T) {
	// Operator passes --prune.distance.blocks=18446744073709551615 (the
	// DefaultBlocksPruneMode magic number) after the auto-upgrade already
	// rewrote Blocks to a finite distance. The shim accepts this reverse
	// transition so the chain-history-expiry policy can be restored without
	// manual DB intervention or a re-sync.
	_, tx := memdb.NewTestTx(t)
	persisted := Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)}
	initStoredMode(t, tx, persisted)

	requested := Mode{Initialised: true, History: Distance(100_000), Blocks: DefaultBlocksPruneMode}
	got, err := EnsureNotChanged(tx, requested)
	require.NoError(t, err)
	assert.Equal(t, requested, got)

	stored, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, requested, stored, "shim must rewrite the persisted value")
}

func TestEnsureNotChanged_BlocksKeepAllToFiniteRejected(t *testing.T) {
	// Narrowing from KeepAllBlocksPruneMode (--prune.mode=blocks or archive)
	// to a finite distance remains rejected — it's a destructive transition
	// that the operator must opt into explicitly (e.g., by switching modes
	// from a fresh datadir).
	_, tx := memdb.NewTestTx(t)
	persisted := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode}
	initStoredMode(t, tx, persisted)

	requested := Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)}
	got, err := EnsureNotChanged(tx, requested)
	require.Error(t, err)
	assert.Equal(t, persisted, got)
}

func TestEnsureNotChanged_ArchiveUnchanged(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	initStoredMode(t, tx, ArchiveMode)

	got, err := EnsureNotChanged(tx, ArchiveMode)
	require.NoError(t, err)
	assert.Equal(t, ArchiveMode, got)
}

func TestEnsureNotChanged_ArbitraryDistanceChangeAccepted(t *testing.T) {
	// Operator passes --prune.distance=500_000 on an existing minimal datadir.
	// Both sides are finite Distance values, so the shim accepts and rewrites.
	_, tx := memdb.NewTestTx(t)
	initStoredMode(t, tx, MinimalMode)

	custom := Mode{Initialised: true, History: Distance(500_000), Blocks: Distance(500_000)}
	got, err := EnsureNotChanged(tx, custom)
	require.NoError(t, err)
	assert.Equal(t, custom, got)

	persisted, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, custom, persisted)
}

func TestEnsureNotChanged_ArchiveDefaultBumpCompat(t *testing.T) {
	// Pre-existing compat path: archive nodes initialized when Blocks defaulted
	// to DefaultBlocksPruneMode must still start under the current ArchiveMode
	// (which uses KeepAllBlocksPruneMode for Blocks).
	_, tx := memdb.NewTestTx(t)
	legacyArchive := Mode{Initialised: true, History: DefaultBlocksPruneMode, Blocks: DefaultBlocksPruneMode}
	initStoredMode(t, tx, legacyArchive)

	got, err := EnsureNotChanged(tx, ArchiveMode)
	require.NoError(t, err)
	assert.Equal(t, ArchiveMode, got)
}
