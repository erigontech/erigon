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
func TestCanonicalView_PromotionSurvivesRisingQuorum(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})

	v.Observe(issuer(1), items("x.seg", "hx"), viewT0)
	require.True(t, v.Observe(issuer(2), items("x.seg", "hx"), viewT0))
	require.True(t, canonicalHas(v, "x.seg", "hx"))

	// Eight more issuers advertise something else — Q climbs to 5.
	for i := 3; i <= 10; i++ {
		v.Observe(issuer(i), items("y.seg", "hy"), viewT0)
	}
	require.True(t, canonicalHas(v, "x.seg", "hx"),
		"Observe never demotes — a rising quorum does not un-promote; explicit Demote is the only demotion path")
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

// TestCanonicalSlot: a slot is the file name with its v<major>.<minor>-
// version prefix stripped, so reversioning variants share a slot.
func TestCanonicalSlot(t *testing.T) {
	require.Equal(t, "accounts.0-2048.kv", CanonicalSlot("v1.0-accounts.0-2048.kv"))
	require.Equal(t, CanonicalSlot("v1.0-accounts.0-2048.kv"), CanonicalSlot("v2.0-accounts.0-2048.kv"),
		"version variants of the same logical file share a slot")
	require.Equal(t, "000000-000500-headers.seg", CanonicalSlot("v1.1-000000-000500-headers.seg"))
	require.Equal(t, "salt-state.txt", CanonicalSlot("salt-state.txt"), "no version prefix → own slot")
}

// TestCanonicalView_CanonicalBySlotReversioning: both version variants of
// a logical file are canonical and group under one slot — the state of a
// reversioning window.
func TestCanonicalView_CanonicalBySlotReversioning(t *testing.T) {
	v := NewCanonicalView(nil, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	for i := 1; i <= 2; i++ {
		v.Observe(issuer(i), items(
			"v1.0-accounts.0-2048.kv", "h1",
			"v2.0-accounts.0-2048.kv", "h2",
		), viewT0)
	}
	variants := v.CanonicalBySlot()["accounts.0-2048.kv"]
	require.Len(t, variants, 2, "both version variants are canonical under one slot")
}

// TestCanonicalView_Demote: a consensus-driven rewind drops the orphaned
// entries; below-boundary entries and genesis are retained, and the
// dropped entry's observations are cleared so a single stale
// re-advertisement cannot revive it.
func TestCanonicalView_Demote(t *testing.T) {
	v := NewCanonicalView(items("v1.0-g.0-100.seg", "gh"), snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	for i := 1; i <= 2; i++ {
		v.Observe(issuer(i), items(
			"v1.0-x.100-200.seg", "hx",
			"v1.0-y.200-300.seg", "hy",
		), viewT0)
	}
	require.True(t, canonicalHas(v, "v1.0-x.100-200.seg", "hx"))
	require.True(t, canonicalHas(v, "v1.0-y.200-300.seg", "hy"))

	// A rewind orphans the range from block 200 up.
	dropped := v.Demote(func(name string) bool { return name == "v1.0-y.200-300.seg" })
	require.Equal(t, 1, dropped)
	require.True(t, canonicalHas(v, "v1.0-x.100-200.seg", "hx"), "below-boundary entry retained")
	require.False(t, canonicalHas(v, "v1.0-y.200-300.seg", "hy"), "orphaned entry demoted")
	require.True(t, canonicalHas(v, "v1.0-g.0-100.seg", "gh"), "genesis never demoted")

	// Observations were cleared — a single stale re-advertisement (below
	// the QFloor of 2) does not revive the demoted entry.
	v.Observe(issuer(1), items("v1.0-y.200-300.seg", "hy"), viewT0)
	require.False(t, canonicalHas(v, "v1.0-y.200-300.seg", "hy"))
}

// TestCanonicalView_Digest: the digest is a content identity — equal for
// equal canonical content regardless of observation order, and it changes
// on demotion.
func TestCanonicalView_Digest(t *testing.T) {
	mk := func() *CanonicalView {
		return NewCanonicalView(items("v1.0-g.0-100.seg", "gh"), snapcfg.QuorumConfig{F: 0.5, QFloor: 2})
	}
	a, b := mk(), mk()
	a.Observe(issuer(1), items("v1.0-x.100-200.seg", "hx"), viewT0)
	a.Observe(issuer(2), items("v1.0-x.100-200.seg", "hx"), viewT0)
	// Same promotions on b, issuers observed in the opposite order.
	b.Observe(issuer(2), items("v1.0-x.100-200.seg", "hx"), viewT0)
	b.Observe(issuer(1), items("v1.0-x.100-200.seg", "hx"), viewT0)
	require.Equal(t, a.Digest(), b.Digest(), "same canonical content → same digest, regardless of observation order")

	before := a.Digest()
	a.Demote(func(name string) bool { return name == "v1.0-x.100-200.seg" })
	require.NotEqual(t, before, a.Digest(), "demotion changes the digest")
}
