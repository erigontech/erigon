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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
)

// TestValidateAdvertisement_AllMatch is the canonical happy path:
// every entry in the advertisement matches an entry in the (single)
// canonical version. Output equals input.
func TestValidateAdvertisement_AllMatch(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "bbb"},
		preverified.Item{Name: "v1.1-000000-001000-transactions.seg", Hash: "ccc"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "bbb"},
	}

	got := ValidateAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	require.Equal(t, adv, got,
		"matching entries pass through unchanged")
}

// TestValidateAdvertisement_HashMismatch pins that a peer advertising
// the same FILE but with a different HASH gets that entry dropped.
// This is the core attack-prevention case: a MITM publisher swaps an
// info-hash to point at a malicious file; we drop the entry.
func TestValidateAdvertisement_HashMismatch(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "good"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "BAD"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}

	got := ValidateAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	require.Equal(t, snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "ok"},
	}, got,
		"only the entry with non-matching hash is dropped; the matching entry survives — per-entry filtering, not wholesale rejection")
}

// TestValidateAdvertisement_UnknownName pins that a peer advertising
// a file canonical doesn't know about gets that entry dropped. The
// peer might have produced a file from a fork or have a bug; we
// don't accept entries we can't verify against canonical.
func TestValidateAdvertisement_UnknownName(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		preverified.Item{Name: "v1.1-999999-999999-headers.seg", Hash: "xxx"},
	}

	got := ValidateAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	require.Equal(t, snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}, got,
		"entry naming a file canonical doesn't know about is dropped")
}

// TestValidateAdvertisement_MultiCanonical_PreMerge_PostMerge pins
// the merge-transition support: with multiple canonical versions
// accepted simultaneously, an entry valid under EITHER passes.
//
// This is the critical Round-B-ready invariant: the helper signature
// already accepts a slice today (single-canonical operation), so
// when the swarm-agreement layer learns to return multiple
// canonicals during a merge, no client code changes.
func TestValidateAdvertisement_MultiCanonical_PreMerge_PostMerge(t *testing.T) {
	t.Parallel()

	// Pre-merge form: two small files covering [0,8192) and [8192,16384).
	preMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
	}
	// Post-merge form: one big file covering [0,16384).
	postMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}

	// Peer A holds pre-merge files.
	peerA := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
	}
	// Peer B holds the post-merge file.
	peerB := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}

	canonicals := []snapcfg.PreverifiedItems{preMerge, postMerge}

	// During the transition, BOTH peers' advertisements validate.
	require.Equal(t, peerA, ValidateAdvertisement(peerA, canonicals),
		"pre-merge peer's advertisement passes when pre-merge canonical is still accepted")
	require.Equal(t, peerB, ValidateAdvertisement(peerB, canonicals),
		"post-merge peer's advertisement passes when post-merge canonical is also accepted")
}

// TestValidateAdvertisement_PostMergeOnly_DropsPreMergePeer pins the
// retirement direction: once the swarm-agreement layer retires the
// pre-merge canonical (leaving only post-merge accepted), a peer
// still advertising pre-merge files gets EVERY pre-merge entry
// dropped. This is how the swarm transitions cleanly: old-form
// holders progressively become irrelevant as canonical narrows.
func TestValidateAdvertisement_PostMergeOnly_DropsPreMergePeer(t *testing.T) {
	t.Parallel()

	postMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-16384.kv", Hash: "postX"},
	}
	peerStillOnPreMerge := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "preA"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-16384.kv", Hash: "preB"},
	}

	got := ValidateAdvertisement(peerStillOnPreMerge, []snapcfg.PreverifiedItems{postMerge})
	require.Nil(t, got,
		"pre-merge peer advertising files no longer in canonical has all entries dropped (returns nil when no entries survive)")
}

// TestValidateAdvertisement_NoCanonical pins that an empty canonical
// set yields nil — there's nothing to validate against. Defensive
// for the "no canonical loaded yet" case.
func TestValidateAdvertisement_NoCanonical(t *testing.T) {
	t.Parallel()

	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	require.Nil(t, ValidateAdvertisement(adv, nil),
		"no canonical means no basis for validation — nil result")
	require.Nil(t, ValidateAdvertisement(adv, []snapcfg.PreverifiedItems{}),
		"empty canonicals slice also returns nil")
}

// TestValidateAdvertisement_EmptyAdv pins the empty-advertisement
// case — input nil → output nil.
func TestValidateAdvertisement_EmptyAdv(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	require.Nil(t, ValidateAdvertisement(nil, []snapcfg.PreverifiedItems{canonical}))
	require.Nil(t, ValidateAdvertisement(snapcfg.PreverifiedItems{}, []snapcfg.PreverifiedItems{canonical}))
}

// TestValidateAdvertisement_DuplicateAdvEntries pins that if the peer
// (somehow) sends duplicate entries for the same file, all duplicates
// that match canonical pass through. This isn't a security concern —
// duplicates are just redundant — but documenting the behaviour
// prevents surprises.
func TestValidateAdvertisement_DuplicateAdvEntries(t *testing.T) {
	t.Parallel()

	canonical := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}
	adv := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "aaa"},
	}

	got := ValidateAdvertisement(adv, []snapcfg.PreverifiedItems{canonical})
	require.Len(t, got, 2,
		"duplicates pass through — dedup is the caller's responsibility, not the validator's")
}
