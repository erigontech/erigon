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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

func TestStaticFilesToFamily_SkipsDisabled(t *testing.T) {
	t.Parallel()
	family, err := staticFilesToFamily(kv.AccountsDomain, StaticFiles{}, 0, 100)
	require.NoError(t, err)
	require.Nil(t, family)
}

func TestStaticFilesToFamily_Complete(t *testing.T) {
	t.Parallel()
	sf := StaticFiles{
		valuesDecomp:    &seg.Decompressor{},
		valuesIdx:       &recsplit.Index{},
		valuesBt:        &btindex.BtIndex{},
		existenceFilter: &existence.Filter{},
		HistoryFiles: HistoryFiles{
			historyDecomp:   &seg.Decompressor{},
			historyIdx:      &recsplit.Index{},
			efHistoryDecomp: &seg.Decompressor{},
			efHistoryIdx:    &recsplit.Index{},
			efExistence:     &existence.Filter{},
		},
	}
	family, err := staticFilesToFamily(kv.StorageDomain, sf, 100, 200)
	require.NoError(t, err)
	require.NotNil(t, family)

	require.Equal(t, kv.StorageDomain, family.Domain)
	require.Equal(t, uint64(100), family.StartTxNum)
	require.Equal(t, uint64(200), family.EndTxNum)

	require.Same(t, sf.valuesDecomp, family.Values.decompressor)
	require.Same(t, sf.valuesIdx, family.Values.index)
	require.Same(t, sf.valuesBt, family.Values.bindex)
	require.Same(t, sf.existenceFilter, family.Values.existence)

	require.Same(t, sf.efHistoryDecomp, family.Index.decompressor)
	require.Same(t, sf.efHistoryIdx, family.Index.index)
	require.Same(t, sf.efExistence, family.Index.existence)

	require.Same(t, sf.historyDecomp, family.History.decompressor)
	require.Same(t, sf.historyIdx, family.History.index)
}

// TestStaticFilesToFamily_ValuesOnlyRejected pins the invariant: a
// StaticFiles with a values .kv but no paired history members MUST NOT
// produce a partial family — that is exactly the mode-D wrong-root
// failure shape.
func TestStaticFilesToFamily_ValuesOnlyRejected(t *testing.T) {
	t.Parallel()
	sf := StaticFiles{
		valuesDecomp: &seg.Decompressor{},
		// no history — the invariant break
	}
	family, err := staticFilesToFamily(kv.AccountsDomain, sf, 0, 100)
	require.ErrorIs(t, err, ErrFileFamilyMissingMember)
	require.Nil(t, family)
}
