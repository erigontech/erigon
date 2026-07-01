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

func TestNamedModesKeepAllCommitmentHistory(t *testing.T) {
	for _, m := range []Mode{ArchiveMode, FullMode, BlocksMode, MinimalMode, MockMode, DefaultMode} {
		assert.Equal(t, KeepAllBlocksPruneMode, m.CommitmentHistory, "named modes must default CommitmentHistory to keep-all")
		assert.False(t, m.CommitmentHistory.Enabled())
		// PruneTo must not panic on a nil BlockAmount.
		assert.Equal(t, uint64(0), m.CommitmentHistory.PruneTo(1_000_000))
	}
}

func TestModeEqualsComparesCommitmentHistory(t *testing.T) {
	a := Mode{Initialised: true, History: Distance(100), Blocks: Distance(100), CommitmentHistory: Distance(50)}
	b := a
	assert.True(t, modeEquals(a, b))
	b.CommitmentHistory = Distance(60)
	assert.False(t, modeEquals(a, b))
}

func TestModeString_CommitmentHistory(t *testing.T) {
	m := ArchiveMode
	m.CommitmentHistory = Distance(100_000)
	assert.Equal(t, "archive --prune.commitment-history.older=100000", m.String())

	// Default (keep-all) commitment history adds no clause.
	assert.Equal(t, "archive", ArchiveMode.String())

	// Legacy blocks shape with a bounded commitment window.
	legacyBlocks := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode, CommitmentHistory: Distance(80_000)}
	assert.Equal(t, "blocks --prune.distance=100000 --prune.commitment-history.older=80000", legacyBlocks.String())
}

func TestModeString_LegacyShapes(t *testing.T) {
	// Pre-EIP-8252 full mode persisted as {Blocks: KeepPostMergeBlocksPruneMode,
	// History: Distance(100_000)}. Before the recognition logic, this rendered
	// as "archive --prune.distance=100000 --prune.distance.blocks=18446744073709551615"
	// — accurate but misleading. It now renders as "full(legacy)".
	legacyFull := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepPostMergeBlocksPruneMode}
	assert.Equal(t, "full(legacy) --prune.distance=100000", legacyFull.String())

	// Pre-EIP-8252 full with the current default history distance (262_144) —
	// label as plain "full(legacy)" with no override clause.
	legacyFullCurrentHistory := Mode{Initialised: true, History: Distance(262_144), Blocks: KeepPostMergeBlocksPruneMode}
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
		mode, err := FromCli(fullModeStr, 0, 0, 0, false)
		assert.NoError(t, err)
		assert.Equal(t, FullMode, mode)

		assert.Equal(t, "full", mode.String())
	})
	t.Run("archive", func(t *testing.T) {
		mode, err := FromCli(archiveModeStr, 0, 0, 0, false)
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

		mode, err := FromCli(archiveModeStr, exp.History.toValue(), exp.Blocks.toValue(), 0, false)
		assert.NoError(t, err)
		assert.Equal(t, exp, mode)
		assert.Equal(t, "archive --prune.distance=400500 --prune.distance.blocks=100500", mode.String())
	})
	t.Run("minimal", func(t *testing.T) {
		mode, err := FromCli(minimalModeStr, 0, 0, 0, false)
		assert.NoError(t, err)
		assert.Equal(t, MinimalMode, mode)
		assert.Equal(t, minimalModeStr, mode.String())
	})
	t.Run("garbage", func(t *testing.T) {
		_, err := FromCli("garb", 1, 2, 0, false)
		assert.ErrorIs(t, err, ErrUnknownPruneMode)
	})
	t.Run("empty", func(t *testing.T) {
		mode, err := FromCli("", 0, 0, 0, false)
		assert.NoError(t, err)

		assert.Equal(t, DefaultMode, mode)
		assert.Equal(t, "archive", mode.String())
	})
}

func TestFromCli_CommitmentHistory(t *testing.T) {
	t.Run("default-unset-keeps-all", func(t *testing.T) {
		mode, err := FromCli(archiveModeStr, 0, 0, 0, false)
		require.NoError(t, err)
		assert.Equal(t, KeepAllBlocksPruneMode, mode.CommitmentHistory)
		assert.False(t, mode.CommitmentHistory.Enabled())
	})
	t.Run("explicit-zero-set-keeps-all", func(t *testing.T) {
		// --prune.commitment-history.older=0 is today's "unlimited" spelling.
		mode, err := FromCli(archiveModeStr, 0, 0, 0, true)
		require.NoError(t, err)
		assert.Equal(t, KeepAllBlocksPruneMode, mode.CommitmentHistory)
	})
	t.Run("bounded-set", func(t *testing.T) {
		mode, err := FromCli(archiveModeStr, 0, 0, 100_000, true)
		require.NoError(t, err)
		assert.Equal(t, Distance(100_000), mode.CommitmentHistory)
		assert.True(t, mode.CommitmentHistory.Enabled())
	})
}

func TestModeValidate(t *testing.T) {
	finite := func(chOlder uint64) Mode {
		m := Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000)}
		if chOlder == 0 {
			m.CommitmentHistory = KeepAllBlocksPruneMode
		} else {
			m.CommitmentHistory = Distance(chOlder)
		}
		return m
	}
	cases := []struct {
		name    string
		mode    Mode
		wantErr bool
	}{
		{"commitment-unlimited-no-constraint", finite(0), false},
		{"within-distance-allowed", finite(50_000), false},
		{"equal-distance-allowed", finite(100_000), false},
		{"exceeds-distance-rejected", finite(200_000), true},
		{"history-unlimited-no-constraint", Mode{Initialised: true, History: KeepAllBlocksPruneMode, Blocks: KeepAllBlocksPruneMode, CommitmentHistory: Distance(200_000)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.mode.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGet_CommitmentHistoryLegacyDefaultsToKeepAll(t *testing.T) {
	// Legacy datadir: only History/Blocks keys were ever written. The missing
	// commitment-history key must resolve to KeepAllBlocksPruneMode.
	_, tx := memdb.NewTestTx(t)
	require.NoError(t, setOnEmpty(tx, kv.PruneHistory, Distance(100_000)))
	require.NoError(t, setOnEmpty(tx, kv.PruneBlocks, Distance(100_000)))

	got, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, KeepAllBlocksPruneMode, got.CommitmentHistory)
}

func TestGet_CommitmentHistoryRoundTrips(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	m := Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000), CommitmentHistory: Distance(80_000)}
	require.NoError(t, overwriteStoredMode(tx, m))

	got, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, Distance(80_000), got.CommitmentHistory)
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
			name:      "blocks KeepPostMergeBlocksPruneMode→finite (full mode EIP-8252 upgrade)",
			persisted: Mode{Initialised: true, History: Distance(100_000), Blocks: KeepPostMergeBlocksPruneMode},
			requested: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)},
			want:      true,
		},
		{
			name:      "blocks finite→KeepPostMergeBlocksPruneMode (revert after auto-upgrade)",
			persisted: Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144)},
			requested: Mode{Initialised: true, History: Distance(100_000), Blocks: KeepPostMergeBlocksPruneMode},
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

func TestIsCommitmentHistoryRetentionPolicy(t *testing.T) {
	assert.True(t, isCommitmentHistoryRetentionPolicy(KeepAllBlocksPruneMode))
	assert.True(t, isCommitmentHistoryRetentionPolicy(Distance(100_000)))
	// KeepPostMergeBlocksPruneMode is meaningless for commitment history.
	assert.False(t, isCommitmentHistoryRetentionPolicy(KeepPostMergeBlocksPruneMode))
}

func TestIsRetentionWindowChange_CommitmentHistory(t *testing.T) {
	base := func(ch BlockAmount) Mode {
		return Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144), CommitmentHistory: ch}
	}
	cases := []struct {
		name      string
		persisted Mode
		requested Mode
		want      bool
	}{
		{"commitment shrink finite→finite", base(Distance(200_000)), base(Distance(100_000)), true},
		{"commitment expand finite→finite", base(Distance(100_000)), base(Distance(200_000)), true},
		{"commitment unlimited→bounded", base(KeepAllBlocksPruneMode), base(Distance(100_000)), true},
		{"commitment bounded→unlimited", base(Distance(100_000)), base(KeepAllBlocksPruneMode), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isRetentionWindowChange(tc.persisted, tc.requested))
		})
	}
}

func TestEnsureNotChanged_CommitmentHistory(t *testing.T) {
	// History finite at 262_144 keeps every commitment window below valid so
	// Validate never trips in these transition tests.
	mk := func(ch BlockAmount) Mode {
		return Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144), CommitmentHistory: ch}
	}

	t.Run("first-set-bounded", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureNotChanged(tx, mk(Distance(100_000)))
		require.NoError(t, err)
		assert.Equal(t, Distance(100_000), got.CommitmentHistory)
	})

	t.Run("shrink-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		initStoredMode(t, tx, mk(Distance(200_000)))
		got, err := EnsureNotChanged(tx, mk(Distance(50_000)))
		require.NoError(t, err)
		assert.Equal(t, Distance(50_000), got.CommitmentHistory)
	})

	// FLIP: expanding a bounded window used to be rejected; now allow+warn.
	t.Run("expand-now-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		initStoredMode(t, tx, mk(Distance(50_000)))
		got, err := EnsureNotChanged(tx, mk(Distance(200_000)))
		require.NoError(t, err)
		assert.Equal(t, Distance(200_000), got.CommitmentHistory)

		persisted, err := Get(tx)
		require.NoError(t, err)
		assert.Equal(t, Distance(200_000), persisted.CommitmentHistory, "shim must rewrite the persisted value")
	})

	t.Run("unlimited-to-bounded-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		initStoredMode(t, tx, mk(KeepAllBlocksPruneMode))
		got, err := EnsureNotChanged(tx, mk(Distance(100_000)))
		require.NoError(t, err)
		assert.Equal(t, Distance(100_000), got.CommitmentHistory)
	})

	// FLIP: bounded→unlimited used to be rejected; now allow+warn.
	t.Run("bounded-to-unlimited-now-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		initStoredMode(t, tx, mk(Distance(50_000)))
		got, err := EnsureNotChanged(tx, mk(KeepAllBlocksPruneMode))
		require.NoError(t, err)
		assert.Equal(t, KeepAllBlocksPruneMode, got.CommitmentHistory)
	})

	t.Run("validates-against-history", func(t *testing.T) {
		// Commitment window wider than state-history retention must be rejected
		// by EnsureNotChanged (Validate backstop), even on first set.
		_, tx := memdb.NewTestTx(t)
		bad := Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000), CommitmentHistory: Distance(200_000)}
		_, err := EnsureNotChanged(tx, bad)
		require.Error(t, err)
	})
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
	legacy := Mode{Initialised: true, History: Distance(100_000), Blocks: Distance(100_000), CommitmentHistory: KeepAllBlocksPruneMode}
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
	// Pre-rescope full mode: {KeepPostMergeBlocksPruneMode (sentinel), Distance(100_000)}.
	// New FullMode has Blocks=Distance(262_144). The shim treats this specific
	// one-way KeepPostMergeBlocksPruneMode→finite transition on Blocks as a
	// retention-window change so existing full nodes upgrade without operator
	// intervention. (Frozen .seg files won't actually be deleted until #21306
	// lands; the config-level transition is still recorded.)
	_, tx := memdb.NewTestTx(t)
	legacyFull := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepPostMergeBlocksPruneMode}
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
	// KeepPostMergeBlocksPruneMode magic number) after the auto-upgrade already
	// rewrote Blocks to a finite distance. The shim accepts this reverse
	// transition so the chain-history-expiry policy can be restored without
	// manual DB intervention or a re-sync.
	_, tx := memdb.NewTestTx(t)
	persisted := Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144), CommitmentHistory: KeepAllBlocksPruneMode}
	initStoredMode(t, tx, persisted)

	requested := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepPostMergeBlocksPruneMode, CommitmentHistory: KeepAllBlocksPruneMode}
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
	persisted := Mode{Initialised: true, History: Distance(100_000), Blocks: KeepAllBlocksPruneMode, CommitmentHistory: KeepAllBlocksPruneMode}
	initStoredMode(t, tx, persisted)

	requested := Mode{Initialised: true, History: Distance(262_144), Blocks: Distance(262_144), CommitmentHistory: KeepAllBlocksPruneMode}
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

	custom := Mode{Initialised: true, History: Distance(500_000), Blocks: Distance(500_000), CommitmentHistory: KeepAllBlocksPruneMode}
	got, err := EnsureNotChanged(tx, custom)
	require.NoError(t, err)
	assert.Equal(t, custom, got)

	persisted, err := Get(tx)
	require.NoError(t, err)
	assert.Equal(t, custom, persisted)
}

func TestEnsureNotChanged_ArchiveDefaultBumpCompat(t *testing.T) {
	// Pre-existing compat path: archive nodes initialized when Blocks defaulted
	// to KeepPostMergeBlocksPruneMode must still start under the current ArchiveMode
	// (which uses KeepAllBlocksPruneMode for Blocks).
	_, tx := memdb.NewTestTx(t)
	legacyArchive := Mode{Initialised: true, History: KeepPostMergeBlocksPruneMode, Blocks: KeepPostMergeBlocksPruneMode}
	initStoredMode(t, tx, legacyArchive)

	got, err := EnsureNotChanged(tx, ArchiveMode)
	require.NoError(t, err)
	assert.Equal(t, ArchiveMode, got)
}
