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

package snapshotsync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
)

// TestCheckOwnAdvertisement_AllMatch pins the happy path: every entry
// has a name in canonical AND a matching hash. nil verdict, nil error.
func TestCheckOwnAdvertisement_AllMatch(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "bbb"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "bbb"},
	}

	verdict, err := CheckOwnAdvertisement(adv, canonical, []snapcfg.PreverifiedItems{canonical})
	require.NoError(t, err, "all entries match canonical — no error")
	require.Nil(t, verdict, "all entries match canonical — no minority verdict")
}

// TestCheckOwnAdvertisement_GenesisDivergence is the FAIL LOUD case: a
// retire bug producing different bytes for a genesis file. Returns
// AdvertisementSelfCheckError — genesis is immutable, so this is fatal.
func TestCheckOwnAdvertisement_GenesisDivergence(t *testing.T) {
	t.Parallel()

	genesis := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "good"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}
	// Publisher's retire produced wrong bytes for a genesis file.
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "BAD"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}

	verdict, err := CheckOwnAdvertisement(adv, genesis, []snapcfg.PreverifiedItems{genesis})
	require.Nil(t, verdict, "a genesis divergence is fatal, not an adoptable minority")
	require.Error(t, err, "hash mismatch on a genesis name returns error")
	require.True(t, IsAdvertisementSelfCheckError(err),
		"error is/wraps AdvertisementSelfCheckError so callers can discriminate")

	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Equal(t, []AdvertisementMismatch{
		{Name: "v1.1-000000-001000-headers.seg", OwnHash: "BAD", CanonicalHash: "good"},
	}, sce.Mismatches)
}

// TestCheckOwnAdvertisement_MinorityVerdict: a mismatch on a name that
// is canonical only via quorum promotion (not in genesis) is a
// non-fatal minority verdict — the node lost the quorum race and must
// adopt the canonical hash.
func TestCheckOwnAdvertisement_MinorityVerdict(t *testing.T) {
	t.Parallel()

	genesis := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "g1"},
	}
	// canonical = genesis ∪ a quorum-promoted post-genesis entry.
	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "g1"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "quorum"},
	}
	// This node retired a different hash for the post-genesis file.
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "g1"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "lost-the-race"},
	}

	verdict, err := CheckOwnAdvertisement(adv, genesis, []snapcfg.PreverifiedItems{canonical})
	require.NoError(t, err, "a quorum-only mismatch is non-fatal")
	require.NotNil(t, verdict)
	require.Equal(t, []AdvertisementMismatch{
		{Name: "v1.1-001000-002000-headers.seg", OwnHash: "lost-the-race", CanonicalHash: "quorum"},
	}, verdict.Adopt)
}

// TestCheckOwnAdvertisement_DivergenceBeatsMinority: when an
// advertisement has both a genesis divergence and a quorum-only
// minority mismatch, the fatal divergence is returned and the minority
// verdict suppressed — the process halts anyway.
func TestCheckOwnAdvertisement_DivergenceBeatsMinority(t *testing.T) {
	t.Parallel()

	genesis := snapcfg.PreverifiedItems{
		preverified.Item{Name: "genesis.seg", Hash: "g"},
	}
	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "genesis.seg", Hash: "g"},
		preverified.Item{Name: "promoted.seg", Hash: "q"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "genesis.seg", Hash: "wrong-genesis"},
		preverified.Item{Name: "promoted.seg", Hash: "wrong-promoted"},
	}

	verdict, err := CheckOwnAdvertisement(adv, genesis, []snapcfg.PreverifiedItems{canonical})
	require.Nil(t, verdict, "minority verdict suppressed when a fatal divergence is present")
	require.True(t, IsAdvertisementSelfCheckError(err))
	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Len(t, sce.Mismatches, 1)
	require.Equal(t, "genesis.seg", sce.Mismatches[0].Name)
}

// TestCheckOwnAdvertisement_NewFileAllowed pins the asymmetry: an entry
// whose NAME is not in any canonical passes silently. Publishers ARE
// the source of new files; requiring canonical match would deadlock
// the swarm-agreement layer.
func TestCheckOwnAdvertisement_NewFileAllowed(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		// New file the publisher just retired; canonical doesn't have it.
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "newbie"},
	}

	verdict, err := CheckOwnAdvertisement(adv, canonical, []snapcfg.PreverifiedItems{canonical})
	require.NoError(t, err, "new file (name not in canonical) passes silently")
	require.Nil(t, verdict)
}

// TestCheckOwnAdvertisement_MultipleMismatchesReported pins that EVERY
// divergent entry is reported, not just the first.
func TestCheckOwnAdvertisement_MultipleMismatchesReported(t *testing.T) {
	t.Parallel()

	genesis := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "g1"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "g2"},
		preverified.Item{Name: "v1.1-002000-003000-headers.seg", Hash: "g3"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "b1"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "b2"},
		preverified.Item{Name: "v1.1-002000-003000-headers.seg", Hash: "b3"},
	}

	_, err := CheckOwnAdvertisement(adv, genesis, []snapcfg.PreverifiedItems{genesis})
	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Len(t, sce.Mismatches, 3,
		"all three divergent entries surfaced, not just the first")
}

// TestCheckOwnAdvertisement_MultiCanonical pins that during a merge
// transition, an entry valid under EITHER canonical passes.
func TestCheckOwnAdvertisement_MultiCanonical(t *testing.T) {
	t.Parallel()

	preMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
	}
	postMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}
	// Publisher in transition holds BOTH forms.
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}

	verdict, err := CheckOwnAdvertisement(adv, preMerge, []snapcfg.PreverifiedItems{preMerge, postMerge})
	require.NoError(t, err, "during merge transition all three entries are valid under one canonical")
	require.Nil(t, verdict)
}

// TestCheckOwnAdvertisement_MultiCanonical_WrongMergeHash pins that a
// hash matching NONE of the accepted canonicals still fails.
func TestCheckOwnAdvertisement_MultiCanonical_WrongMergeHash(t *testing.T) {
	t.Parallel()

	preMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "preX"},
	}
	postMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postY"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "wrong"},
	}

	_, err := CheckOwnAdvertisement(adv, preMerge, []snapcfg.PreverifiedItems{preMerge, postMerge})
	require.True(t, IsAdvertisementSelfCheckError(err),
		"hash matching NEITHER canonical version still fails")
}

// TestCheckOwnAdvertisement_NoCanonical pins defensive behaviour:
// empty canonical list returns (nil, nil).
func TestCheckOwnAdvertisement_NoCanonical(t *testing.T) {
	t.Parallel()

	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	verdict, err := CheckOwnAdvertisement(adv, nil, nil)
	require.NoError(t, err)
	require.Nil(t, verdict)

	verdict, err = CheckOwnAdvertisement(adv, nil, []snapcfg.PreverifiedItems{})
	require.NoError(t, err)
	require.Nil(t, verdict)
}

// TestCheckOwnAdvertisement_EmptyAdv pins empty-advertisement case.
func TestCheckOwnAdvertisement_EmptyAdv(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	verdict, err := CheckOwnAdvertisement(nil, canonical, []snapcfg.PreverifiedItems{canonical})
	require.NoError(t, err)
	require.Nil(t, verdict)
}

// TestAdvertisementSelfCheckError_Truncates pins the error-message
// formatting: many mismatches show up to 8 and a "N more" tail.
func TestAdvertisementSelfCheckError_Truncates(t *testing.T) {
	t.Parallel()

	genesis := snapcfg.PreverifiedItems{}
	adv := snapcfg.PreverifiedItems{}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("v1.1-%06d-%06d-headers.seg", i*1000, (i+1)*1000)
		genesis = append(genesis, preverified.Item{Name: name, Hash: "good"})
		adv = append(adv, preverified.Item{Name: name, Hash: "bad"})
	}

	_, err := CheckOwnAdvertisement(adv, genesis, []snapcfg.PreverifiedItems{genesis})
	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Len(t, sce.Mismatches, 20, "full mismatch list available programmatically")
	require.Contains(t, sce.Error(), "20 entries diverge", "summary reports total count")
	require.Contains(t, sce.Error(), "12 more", "message indicates remaining entries")
}
