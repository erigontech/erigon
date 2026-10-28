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
