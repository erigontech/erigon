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
// has a name in canonical AND a matching hash. nil error.
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

	require.NoError(t, CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{canonical}),
		"all entries match canonical — no error")
}

// TestCheckOwnAdvertisement_HashMismatch is the FAIL LOUD case: a
// retire bug producing different bytes for a known canonical file.
// Returns AdvertisementSelfCheckError with the mismatch details.
func TestCheckOwnAdvertisement_HashMismatch(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "good"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}
	// Publisher's retire produced wrong bytes for headers.
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "BAD"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}

	err := CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	require.Error(t, err, "hash mismatch on known name returns error")
	require.True(t, IsAdvertisementSelfCheckError(err),
		"error is/wraps AdvertisementSelfCheckError so callers can discriminate from generic errors")

	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Equal(t, []AdvertisementMismatch{
		{
			Name:          "v1.1-000000-001000-headers.seg",
			OwnHash:       "BAD",
			CanonicalHash: "good",
		},
	}, sce.Mismatches)
}

// TestCheckOwnAdvertisement_NewFileAllowed pins the asymmetry from
// the design: an entry whose NAME is not in any canonical passes
// silently. Publishers ARE the source of new files; requiring
// canonical match would deadlock the swarm-agreement layer (nothing
// new could ever enter canonical).
func TestCheckOwnAdvertisement_NewFileAllowed(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		// New file the publisher just retired; canonical doesn't have it yet.
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "newbie"},
	}

	require.NoError(t, CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{canonical}),
		"new file (name not in canonical) passes silently — these are normal retire outputs awaiting promotion")
}

// TestCheckOwnAdvertisement_MultipleMismatchesReported pins that EVERY
// divergent entry is reported, not just the first. Operators need
// the full picture to diagnose whether the retire bug is global or
// scoped.
func TestCheckOwnAdvertisement_MultipleMismatchesReported(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "g1"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "g2"},
		preverified.Item{Name: "v1.1-002000-003000-headers.seg", Hash: "g3"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "b1"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "b2"},
		preverified.Item{Name: "v1.1-002000-003000-headers.seg", Hash: "b3"},
	}

	err := CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Len(t, sce.Mismatches, 3,
		"all three divergent entries surfaced, not just the first — operator needs full picture")
}

// TestCheckOwnAdvertisement_MultiCanonical pins that during a merge
// transition, an entry valid under EITHER canonical passes. The
// producer's advertisement may contain pre-merge files (still valid
// while pre-merge canonical accepted) or post-merge file (valid
// when post-merge canonical accepted) — both should pass during
// the transition window.
func TestCheckOwnAdvertisement_MultiCanonical(t *testing.T) {
	t.Parallel()

	preMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
	}
	postMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}

	// Publisher in transition holds BOTH forms (uploaded pre-merge,
	// just produced post-merge merger output).
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}

	require.NoError(t, CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{preMerge, postMerge}),
		"during merge transition, all three entries are valid under one or the other canonical")
}

// TestCheckOwnAdvertisement_MultiCanonical_WrongMergeHash pins that
// even with multiple canonicals accepted, a hash that matches NONE
// of them fails. A publisher producing wrong merger output is
// still a retire bug.
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

	err := CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{preMerge, postMerge})
	require.True(t, IsAdvertisementSelfCheckError(err),
		"hash matching NEITHER canonical version still fails — multi-canonical accepts ANY of the agreed hashes, not arbitrary other hashes")
}

// TestCheckOwnAdvertisement_NoCanonical pins defensive behaviour:
// empty canonical list returns nil (no basis to check). Production
// callers must ensure canonical is loaded; helper is defensive
// rather than erroring on missing input.
func TestCheckOwnAdvertisement_NoCanonical(t *testing.T) {
	t.Parallel()

	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	require.NoError(t, CheckOwnAdvertisement(adv, nil))
	require.NoError(t, CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{}))
}

// TestCheckOwnAdvertisement_EmptyAdv pins empty-advertisement case.
func TestCheckOwnAdvertisement_EmptyAdv(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	require.NoError(t, CheckOwnAdvertisement(nil, []snapcfg.PreverifiedItems{canonical}))
}

// TestAdvertisementSelfCheckError_Truncates pins the error-message
// formatting: when there are many mismatches, the message shows up
// to 8 and indicates how many more are in the Mismatches slice.
// This keeps log lines bounded while preserving the full structured
// data for programmatic inspection.
func TestAdvertisementSelfCheckError_Truncates(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{}
	adv := snapcfg.PreverifiedItems{}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("v1.1-%06d-%06d-headers.seg", i*1000, (i+1)*1000)
		canonical = append(canonical, preverified.Item{Name: name, Hash: "good"})
		adv = append(adv, preverified.Item{Name: name, Hash: "bad"})
	}

	err := CheckOwnAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	var sce *AdvertisementSelfCheckError
	require.True(t, errors.As(err, &sce))
	require.Len(t, sce.Mismatches, 20,
		"full mismatch list available programmatically")
	require.Contains(t, sce.Error(), "20 entries diverge",
		"summary in error message reports total count")
	require.Contains(t, sce.Error(), "12 more",
		"message indicates remaining entries beyond the truncated display")
}
