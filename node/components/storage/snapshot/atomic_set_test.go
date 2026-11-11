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

package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccessorsOf_PairsAcrossIndependentVersionPrefixes(t *testing.T) {
	got := AccessorsOf("domain/v2.2-commitment.355-356.kv", []string{
		"domain/v2.1-commitment.355-356.kvi",
	})
	require.Equal(t, []string{"domain/v2.1-commitment.355-356.kvi"}, got)
}

// A coordinate also groups primaries that are built independently of each
// other. Discarding a domain .kv must not take the history .v or index .ef
// with it.
func TestAccessorsOf_SkipsSameCoordinatePrimaries(t *testing.T) {
	got := AccessorsOf("domain/v2.0-accounts.355-356.kv", []string{
		"domain/v2.0-accounts.355-356.bt",
		"domain/v1.2-accounts.355-356.kvei",
		"history/v2.1-accounts.355-356.v",
		"idx/v3.1-accounts.355-356.ef",
		"accessor/v1.2-accounts.355-356.vi",
		"accessor/v2.1-accounts.355-356.efi",
	})
	require.Equal(t, []string{
		"domain/v2.0-accounts.355-356.bt",
		"domain/v1.2-accounts.355-356.kvei",
	}, got)
}

// A segment's indexes are named after the index, not the segment: a
// transactions .seg owns transactions.idx and transactions-to-block.idx. The
// second carries a different type string, so matching the segment's own name
// finds only half the set and leaves the other half to be measured against a
// hash of bytes that are gone.
func TestAccessorsOf_SegmentTakesEveryIndexItsTypeDeclares(t *testing.T) {
	got := AccessorsOf("v1.1-003960-003970-transactions.seg", []string{
		"v2.0-003960-003970-transactions.idx",
		"v2.0-003960-003970-transactions-to-block.idx",
	})
	require.ElementsMatch(t, []string{
		"v2.0-003960-003970-transactions.idx",
		"v2.0-003960-003970-transactions-to-block.idx",
	}, got)
}

// The broadened name set is the declaring type's own, so a headers segment
// does not acquire another type's index over the same range.
func TestAccessorsOf_SegmentDoesNotTakeAnotherTypesIndex(t *testing.T) {
	got := AccessorsOf("v1.1-003960-003970-headers.seg", []string{
		"v2.0-003960-003970-headers.idx",
		"v2.0-003960-003970-bodies.idx",
		"v2.0-003960-003970-transactions-to-block.idx",
	})
	require.Equal(t, []string{"v2.0-003960-003970-headers.idx"}, got)
}

func TestAccessorsOf_SkipsOtherRanges(t *testing.T) {
	got := AccessorsOf("domain/v2.2-commitment.355-356.kv", []string{
		"domain/v2.1-commitment.354-355.kvi",
		"domain/v2.1-commitment.355-357.kvi",
	})
	require.Empty(t, got)
}

func TestAccessorsOf_HistoryAndIdxTakeOnlyTheirOwnAccessor(t *testing.T) {
	require.Equal(t, []string{"accessor/v1.2-accounts.355-356.vi"},
		AccessorsOf("history/v2.1-accounts.355-356.v", []string{
			"accessor/v1.2-accounts.355-356.vi",
			"accessor/v2.1-accounts.355-356.efi",
		}))
	require.Equal(t, []string{"accessor/v2.1-accounts.355-356.efi"},
		AccessorsOf("idx/v3.1-accounts.355-356.ef", []string{
			"accessor/v1.2-accounts.355-356.vi",
			"accessor/v2.1-accounts.355-356.efi",
		}))
}

func TestAccessorsOf_UnknownPrimaryHasNoSet(t *testing.T) {
	require.Empty(t, AccessorsOf("salt-state.txt", []string{"salt-state.txt"}))
}

// Asking the disk per primary costs a directory read and a name parse per
// candidate, so a sweep over N primaries re-reads the same directories N times
// and parses N×M names. The index reads them once and answers by coordinate.
func TestAccessorIndex_AnswersAsThePerFileScanDoes(t *testing.T) {
	snapDir := t.TempDir()
	files := []string{
		"v1.1-025050-025060-headers.seg",
		"v2.0-025050-025060-headers.idx",
		"v1.1-025060-025070-headers.seg",
		"domain/v2.2-commitment.355-356.kv",
		"domain/v2.1-commitment.355-356.kvi",
		"history/v2.1-accounts.355-356.v",
	}
	for _, f := range files {
		path := filepath.Join(snapDir, filepath.FromSlash(f))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
	}

	ix := NewAccessorIndex(snapDir)
	for _, primary := range []string{
		"v1.1-025050-025060-headers.seg",
		"v1.1-025060-025070-headers.seg",
		"domain/v2.2-commitment.355-356.kv",
		"history/v2.1-accounts.355-356.v",
	} {
		require.Equal(t, len(AccessorsOnDisk(snapDir, primary)) > 0, ix.Has(primary),
			"index disagrees with the per-file scan for %s", primary)
	}
}

func TestAccessorIndex_EmptyDirHasNothing(t *testing.T) {
	ix := NewAccessorIndex(t.TempDir())
	require.False(t, ix.Has("v1.1-025050-025060-headers.seg"))
}
