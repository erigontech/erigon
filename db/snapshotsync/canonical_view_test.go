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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
)

var viewT0 = time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)

func issuer(n int) []byte { return []byte{byte(n)} }

func items(pairs ...string) snapcfg.PreverifiedItems {
	out := make(snapcfg.PreverifiedItems, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, snapcfg.PreverifiedItem{Name: pairs[i], Hash: pairs[i+1]})
	}
	return out
}

func canonicalHas(v *CanonicalView, name, hash string) bool {
	for _, it := range v.Canonical() {
		if it.Name == name && it.Hash == hash {
			return true
		}
	}
	return false
}

func countName(v *CanonicalView, name string) int {
	n := 0
	for _, it := range v.Canonical() {
		if it.Name == name {
			n++
		}
	}
	return n
}

// TestCanonicalView_PromotesAtFloor: with QFloor=2, an entry needs two
// distinct issuers; one issuer is below the floor.
func TestCanonicalView_PromotesAtFloor(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	e := items("a.seg", "h1")

	require.False(t, v.Observe(issuer(1), e, viewT0))
	require.False(t, canonicalHas(v, "a.seg", "h1"))
	require.Equal(t, 0, v.Version())

	require.True(t, v.Observe(issuer(2), e, viewT0))
	require.True(t, canonicalHas(v, "a.seg", "h1"))
	require.Equal(t, 1, v.Version())

	// Re-observing by an already-counted issuer promotes nothing new.
	require.False(t, v.Observe(issuer(2), e, viewT0))
	require.Equal(t, 1, v.Version())
}

// TestCanonicalView_FractionRaisesQuorum: with QFloor=1 the fraction
// dominates. Once 10 distinct issuers have been observed (Q =
// ceil(0.5*10) = 5), an entry that only 2 of them advertise does not
// reach canonical.
func TestCanonicalView_FractionRaisesQuorum(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 1})

	// Build the population to N=10 first.
	for i := 1; i <= 10; i++ {
		v.Observe(issuer(i), items("y.seg", "hy"), viewT0)
	}
	// x.seg is introduced late, advertised by only 2 of the 10.
	v.Observe(issuer(1), items("x.seg", "hx"), viewT0)
	v.Observe(issuer(2), items("x.seg", "hx"), viewT0)

	require.False(t, canonicalHas(v, "x.seg", "hx"), "2 of 10 issuers is below Q=5")
	require.True(t, canonicalHas(v, "y.seg", "hy"), "all 10 issuers clears Q=5")
}

// TestCanonicalView_MonotonicNoDemote: an entry promoted at a low Q
// stays canonical after Q rises past its issuer count.
func TestCanonicalView_MonotonicNoDemote(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})

	v.Observe(issuer(1), items("x.seg", "hx"), viewT0)
	require.True(t, v.Observe(issuer(2), items("x.seg", "hx"), viewT0))
	require.True(t, canonicalHas(v, "x.seg", "hx"))

	// Eight more issuers advertise something else — Q climbs to 5.
	for i := 3; i <= 10; i++ {
		v.Observe(issuer(i), items("y.seg", "hy"), viewT0)
	}
	require.True(t, canonicalHas(v, "x.seg", "hx"), "promotion is monotonic — never demoted")
}

// TestCanonicalView_GenesisAlwaysCanonical: genesis entries are
// canonical without quorum and survive GC.
func TestCanonicalView_GenesisAlwaysCanonical(t *testing.T) {
	v := NewCanonicalView(items("g.seg", "gh"), snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	v.SetGCWindow(time.Hour)

	require.True(t, canonicalHas(v, "g.seg", "gh"))
	v.GC(viewT0.Add(48 * time.Hour))
	require.True(t, canonicalHas(v, "g.seg", "gh"), "genesis is never GC'd")
}

// TestCanonicalView_MergeFormsCoexist: pre-merge and merged hashes for
// the same name accumulate quorum independently; both stay canonical.
func TestCanonicalView_MergeFormsCoexist(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})

	for i := 1; i <= 2; i++ {
		v.Observe(issuer(i), items("r.seg", "premerge"), viewT0)
	}
	for i := 3; i <= 4; i++ {
		v.Observe(issuer(i), items("r.seg", "merged"), viewT0)
	}
	require.True(t, canonicalHas(v, "r.seg", "premerge"))
	require.True(t, canonicalHas(v, "r.seg", "merged"))
	require.Equal(t, 2, countName(v, "r.seg"))
}

// TestCanonicalView_GCDropsStaleEntry: an entry no issuer has
// advertised within the window is dropped along with its promotion.
func TestCanonicalView_GCDropsStaleEntry(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	v.SetGCWindow(time.Hour)

	v.Observe(issuer(1), items("s.seg", "hs"), viewT0)
	v.Observe(issuer(2), items("s.seg", "hs"), viewT0)
	require.True(t, canonicalHas(v, "s.seg", "hs"))

	require.Equal(t, 0, v.GC(viewT0.Add(30*time.Minute)), "within window — nothing dropped")
	require.True(t, canonicalHas(v, "s.seg", "hs"))

	require.Equal(t, 1, v.GC(viewT0.Add(2*time.Hour)), "past window — dropped")
	require.False(t, canonicalHas(v, "s.seg", "hs"))

	// A fresher observation of one issuer keeps the entry alive.
	v.Observe(issuer(1), items("s.seg", "hs"), viewT0.Add(3*time.Hour))
	v.Observe(issuer(2), items("s.seg", "hs"), viewT0.Add(3*time.Hour))
	require.Equal(t, 0, v.GC(viewT0.Add(3*time.Hour+30*time.Minute)))
	require.True(t, canonicalHas(v, "s.seg", "hs"))
}

// TestCanonicalView_VersionAdvancesPerBatch: the version counter
// advances once per Observe that promotes new entries, not otherwise.
func TestCanonicalView_VersionAdvancesPerBatch(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})

	require.Equal(t, 0, v.Version())
	v.Observe(issuer(1), items("a.seg", "ha", "b.seg", "hb"), viewT0)
	require.Equal(t, 0, v.Version(), "single issuer promotes nothing")

	v.Observe(issuer(2), items("a.seg", "ha", "b.seg", "hb"), viewT0)
	require.Equal(t, 1, v.Version(), "one batch promoted a.seg and b.seg together")

	v.Observe(issuer(3), items("a.seg", "ha"), viewT0)
	require.Equal(t, 1, v.Version(), "no new entry — version unchanged")
}

// TestCanonicalView_StateRoundTrip: MarshalState/RestoreState preserves
// promotions, the version counter, the issuer set, and partial
// (not-yet-quorum) observations.
func TestCanonicalView_StateRoundTrip(t *testing.T) {
	v := NewCanonicalView(items("g.seg", "gh"), snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	v.Observe(issuer(1), items("a.seg", "ha"), viewT0)
	v.Observe(issuer(2), items("a.seg", "ha"), viewT0) // a.seg promoted
	v.Observe(issuer(3), items("b.seg", "hb"), viewT0) // b.seg seen once only
	require.Equal(t, 1, v.Version())

	blob, err := v.MarshalState()
	require.NoError(t, err)

	restored := NewCanonicalView(items("g.seg", "gh"), snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	require.NoError(t, restored.RestoreState(blob))

	require.Equal(t, 1, restored.Version())
	require.True(t, canonicalHas(restored, "g.seg", "gh"))
	require.True(t, canonicalHas(restored, "a.seg", "ha"))
	require.False(t, canonicalHas(restored, "b.seg", "hb"))

	// b.seg's single restored sighting still counts — a second distinct
	// issuer promotes it.
	require.True(t, restored.Observe(issuer(4), items("b.seg", "hb"), viewT0))
	require.True(t, canonicalHas(restored, "b.seg", "hb"))
	require.Equal(t, 2, restored.Version())

	// issuer(3) was restored into the issuer set — re-observing adds nothing.
	require.False(t, restored.Observe(issuer(3), items("b.seg", "hb"), viewT0))
}

// TestCanonicalView_RestoreRejectsCorrupt: a malformed snapshot is an
// error so the caller can log and start fresh.
func TestCanonicalView_RestoreRejectsCorrupt(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	require.Error(t, v.RestoreState([]byte("not json")))
}

// TestCanonicalView_DedupGenesisAndPromoted: an entry identical to a
// genesis (name, hash) pair is not duplicated when also promoted.
func TestCanonicalView_DedupGenesisAndPromoted(t *testing.T) {
	v := NewCanonicalView(items("g.seg", "gh"), snapcfg.QuorumConfig{F: 0.5, QFloor: 2})

	v.Observe(issuer(1), items("g.seg", "gh"), viewT0)
	v.Observe(issuer(2), items("g.seg", "gh"), viewT0)
	require.Equal(t, 1, countName(v, "g.seg"), "genesis entry not duplicated by promotion")
}
