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

package downloader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortItemsLatestFirst_MinimumBeforeExtrasWithinSameStep(t *testing.T) {
	t.Parallel()
	// Same step, mix of minimum and extras. Minimum (kv, kvi) goes
	// first; history (.v) goes after even though same step range.
	items := []preverifiedSnapshot{
		{Name: "v1.0-accountsHistory.0-256.v"},
		{Name: "v1.0-accounts.0-256.kv"},
		{Name: "v1.0-accountsHistory.0-256.ef"},
		{Name: "v1.0-accounts.0-256.kvi"},
	}
	sortItemsLatestFirst(items)
	// First two are the minimum subset (kv + kvi). Order between them
	// is stable (input order preserved at equal sort key).
	minimum := []string{items[0].Name, items[1].Name}
	require.ElementsMatch(t,
		[]string{"v1.0-accounts.0-256.kv", "v1.0-accounts.0-256.kvi"},
		minimum, "minimum-tier files come first")
	// Last two are extras.
	extras := []string{items[2].Name, items[3].Name}
	require.ElementsMatch(t,
		[]string{"v1.0-accountsHistory.0-256.v", "v1.0-accountsHistory.0-256.ef"},
		extras, "extras-tier files come after minimum")
}

func TestSortItemsLatestFirst_MinimumLatestStepBeatsExtrasOlderStep(t *testing.T) {
	t.Parallel()
	// Minimum file at OLDER step still beats extras file at NEWER
	// step — the IsMinimum tier outranks the To-desc tiebreak.
	items := []preverifiedSnapshot{
		{Name: "v1.0-accountsHistory.512-768.v"}, // extras, latest
		{Name: "v1.0-accounts.0-256.kv"},         // minimum, oldest
	}
	sortItemsLatestFirst(items)
	require.Equal(t, "v1.0-accounts.0-256.kv", items[0].Name,
		"minimum @ older step beats extras @ newer step")
	require.Equal(t, "v1.0-accountsHistory.512-768.v", items[1].Name)
}

func TestSortItemsLatestFirst_StateFiles(t *testing.T) {
	t.Parallel()
	items := []preverifiedSnapshot{
		{Name: "v1.0-accounts.0-256.kv"},
		{Name: "v1.0-accounts.512-768.kv"},
		{Name: "v1.0-accounts.256-512.kv"},
	}
	sortItemsLatestFirst(items)
	require.Equal(t, "v1.0-accounts.512-768.kv", items[0].Name)
	require.Equal(t, "v1.0-accounts.256-512.kv", items[1].Name)
	require.Equal(t, "v1.0-accounts.0-256.kv", items[2].Name)
}

func TestSortItemsLatestFirst_UnparseableGoesToEnd(t *testing.T) {
	t.Parallel()
	items := []preverifiedSnapshot{
		{Name: "erigondb.toml"},             // unparseable (no version prefix)
		{Name: "v1.0-accounts.0-256.kv"},    // parseable, To=256
		{Name: "totally-bogus-name"},        // unparseable
		{Name: "v1.0-accounts.768-1024.kv"}, // parseable, latest
	}
	sortItemsLatestFirst(items)
	// Two parseable state files come first, latest first.
	require.Equal(t, "v1.0-accounts.768-1024.kv", items[0].Name)
	require.Equal(t, "v1.0-accounts.0-256.kv", items[1].Name)
	// Two unparseable items go to the end (relative order preserved).
	require.Contains(t, []string{"erigondb.toml", "totally-bogus-name"}, items[2].Name)
	require.Contains(t, []string{"erigondb.toml", "totally-bogus-name"}, items[3].Name)
	require.Equal(t, "erigondb.toml", items[2].Name,
		"stable sort preserves input order for unparseables")
	require.Equal(t, "totally-bogus-name", items[3].Name)
}

func TestSortItemsLatestFirst_PreservesOrderForEqualKeys(t *testing.T) {
	t.Parallel()
	// Files with the same (From, To) — different domains/types. Stable
	// sort must preserve their input order.
	items := []preverifiedSnapshot{
		{Name: "v1.0-accounts.256-512.kv"},
		{Name: "v1.0-storage.256-512.kv"},
		{Name: "v1.0-code.256-512.kv"},
	}
	sortItemsLatestFirst(items)
	require.Equal(t, "v1.0-accounts.256-512.kv", items[0].Name)
	require.Equal(t, "v1.0-storage.256-512.kv", items[1].Name)
	require.Equal(t, "v1.0-code.256-512.kv", items[2].Name)
}

func TestSortItemsLatestFirst_EmptyAndSingle(t *testing.T) {
	t.Parallel()
	// Empty slice — must not panic.
	var empty []preverifiedSnapshot
	sortItemsLatestFirst(empty)
	require.Empty(t, empty)

	// Single item — must not panic.
	single := []preverifiedSnapshot{{Name: "v1.1-001000-001500-headers.seg"}}
	sortItemsLatestFirst(single)
	require.Len(t, single, 1)
}
