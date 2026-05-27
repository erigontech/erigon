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
	"time"

	"github.com/stretchr/testify/require"
)

func TestInventoryAddAndCoverage(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain:   DomainAccounts,
		FromStep: 0, ToStep: 1024,
		Name:  "v1.0-accounts.0-1024.kv",
		Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{
		Domain:   DomainAccounts,
		FromStep: 1024, ToStep: 2048,
		Name:  "v1.0-accounts.1024-2048.kv",
		Local: true, Trust: TrustVerified,
	})

	cov := inv.Coverage(DomainAccounts)
	require.True(t, cov.IsComplete(0, 2048))
	require.Equal(t, uint64(2048), cov.Coverage())
}

func TestInventoryGapsAgainst(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain:   DomainAccounts,
		FromStep: 0, ToStep: 1024,
		Name:  "v1.0-accounts.0-1024.kv",
		Local: true, Trust: TrustVerified,
	})

	remote := StepRanges{{0, 2048}}
	gaps := inv.GapsAgainst(DomainAccounts, remote)
	require.Equal(t, StepRanges{{1024, 2048}}, gaps)
}

func TestInventoryReplaceWithMerge(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 512,
		Name: "v1.0-accounts.0-512.kv", Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 512, ToStep: 1024,
		Name: "v1.0-accounts.512-1024.kv", Local: true, Trust: TrustVerified,
	})

	merged := &FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 1024,
		Name: "v1.0-accounts.0-1024.kv", Local: true, Trust: TrustVerified,
	}
	ok := inv.ReplaceWithMerge(merged, []string{
		"v1.0-accounts.0-512.kv",
		"v1.0-accounts.512-1024.kv",
	})
	require.True(t, ok)

	files := inv.AllDomainFiles(DomainAccounts)
	require.Len(t, files, 1)
	require.Equal(t, "v1.0-accounts.0-1024.kv", files[0].Name)
}

func TestInventoryReplaceWithMergeRejectsInsufficientCoverage(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 1024,
		Name: "v1.0-accounts.0-1024.kv", Local: true, Trust: TrustVerified,
	})

	// Merged file doesn't cover the existing file's range.
	merged := &FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 512,
		Name: "v1.0-accounts.0-512.kv", Local: true, Trust: TrustVerified,
	}
	ok := inv.ReplaceWithMerge(merged, []string{"v1.0-accounts.0-1024.kv"})
	require.False(t, ok)
}

func TestInventoryTrustPromotion(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 1024,
		Name: "v1.0-accounts.0-1024.kv", Trust: TrustNone,
	})

	changed := inv.PromoteTrust("v1.0-accounts.0-1024.kv", TrustConsensus)
	require.True(t, changed)

	files := inv.AllDomainFiles(DomainAccounts)
	require.Equal(t, TrustConsensus, files[0].Trust)

	// Can't demote.
	changed = inv.PromoteTrust("v1.0-accounts.0-1024.kv", TrustNone)
	require.False(t, changed)
	require.Equal(t, TrustConsensus, inv.AllDomainFiles(DomainAccounts)[0].Trust)
}

func TestInventoryCoverageAtTrust(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 1024,
		Name: "verified.kv", Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 1024, ToStep: 2048,
		Name: "consensus.kv", Local: true, Trust: TrustConsensus,
	})
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 2048, ToStep: 3072,
		Name: "none.kv", Local: true, Trust: TrustNone,
	})

	// All trust levels.
	cov := inv.CoverageAtTrust(DomainAccounts, TrustNone)
	require.Equal(t, uint64(3072), cov.Coverage())

	// Consensus and above.
	cov = inv.CoverageAtTrust(DomainAccounts, TrustConsensus)
	require.Equal(t, uint64(2048), cov.Coverage())

	// Verified only.
	cov = inv.CoverageAtTrust(DomainAccounts, TrustVerified)
	require.Equal(t, uint64(1024), cov.Coverage())
}

func TestInventoryLocalVsRemote(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 1024,
		Name: "local.kv", Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 1024, ToStep: 2048,
		Name: "remote.kv", Local: false, Trust: TrustConsensus,
	})

	// Local coverage only includes local files.
	localCov := inv.Coverage(DomainAccounts)
	require.Equal(t, uint64(1024), localCov.Coverage())

	// Full coverage includes both.
	fullCov := inv.FullCoverage(DomainAccounts)
	require.Equal(t, uint64(2048), fullCov.Coverage())

	// LocalFiles returns only local.
	local := inv.LocalFiles(DomainAccounts)
	require.Len(t, local, 1)
	require.Equal(t, "local.kv", local[0].Name)
}

func TestInventoryBlockFiles(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Name: "v1.0-000000-000500-headers.seg", Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{
		Name: "v1.0-000000-000500-bodies.seg", Local: true, Trust: TrustVerified,
	})

	blocks := inv.BlockFiles()
	require.Len(t, blocks, 2)
}

// TestInventoryViewBlockFilesIncludesAccessors is the regression for the
// publisher wedge where InitialStateReady hung forever: InventoryView.
// BlockFiles filtered to KindKV, dropping block .idx accessors. The
// lifecycle Sweep iterates view.BlockFiles, so it never dispatched the
// accessors, they never reached LifecycleIndexed, and the orchestrator's
// tryFireInitialStateReady (which waits on every phase-1 file, accessors
// included) blocked permanently.
func TestInventoryViewBlockFilesIncludesAccessors(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "v1.0-000000-000500-headers.seg", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{Name: "v2.0-000000-000500-headers.idx", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{Kind: KindMeta, Name: "erigondb.toml", Local: true, Trust: TrustVerified})

	// Precondition: the .idx is classified as a block accessor.
	idx, ok := inv.GetByName("v2.0-000000-000500-headers.idx")
	require.True(t, ok)
	require.Equal(t, KindAccessor, idx.Kind)
	require.Equal(t, Domain(""), idx.Domain)

	view := inv.View()
	defer view.Close()

	names := make([]string, 0, 2)
	for _, e := range view.BlockFiles() {
		names = append(names, e.Name)
	}
	require.ElementsMatch(t,
		[]string{"v1.0-000000-000500-headers.seg", "v2.0-000000-000500-headers.idx"},
		names, "view.BlockFiles must include the block .idx accessor, not just the .seg primary; meta stays excluded")
}

// TestInventoryNonKVKinds covers the new caplin/meta/salt slices. Each kind
// routes to its own slice via AddFile and is returned by its accessor.
// History + idx kinds stay under the domain map alongside kv but distinct
// per Kind, so CoverageOfKind can detect a missing-history gap even when
// kv coverage is complete.
func TestInventoryNonKVKinds(t *testing.T) {
	inv := NewInventory()

	// Caplin, meta, salt — each goes to its own bucket and back-out via the kind accessor.
	inv.AddFile(&FileEntry{Kind: KindCaplin, Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{Kind: KindMeta, Name: "erigondb.toml", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{Kind: KindSalt, Name: "salt-blocks.txt", Local: true, Trust: TrustVerified})

	require.Len(t, inv.CaplinFiles(), 1)
	require.Len(t, inv.MetaFiles(), 1)
	require.Len(t, inv.SaltFiles(), 1)
	require.Len(t, inv.BlockFiles(), 0, "non-block kinds must not leak into BlockFiles")

	// History + idx land under the domain map. Kv full-coverage masks the
	// missing-history gap unless callers use CoverageOfKind.
	inv.AddFile(&FileEntry{Domain: DomainAccounts, FromStep: 0, ToStep: 128, Name: "v1.1-accounts.0-128.kv", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{Domain: DomainAccounts, FromStep: 0, ToStep: 128, Kind: KindHistory, Name: "v1.1-accounts.0-128.v", Local: true, Trust: TrustVerified})

	kvCov := inv.CoverageOfKind(DomainAccounts, KindKV)
	histCov := inv.CoverageOfKind(DomainAccounts, KindHistory)
	idxCov := inv.CoverageOfKind(DomainAccounts, KindIdx)

	require.Equal(t, StepRanges{{From: 0, To: 128}}, kvCov)
	require.Equal(t, StepRanges{{From: 0, To: 128}}, histCov)
	require.Empty(t, idxCov, "idx kind absent — coverage must be empty even though kv covers the same range")

	// LocalFiles returns every kind for the domain, callers can filter by Kind.
	all := inv.LocalFiles(DomainAccounts)
	require.Len(t, all, 2)

	// RemoveFile must scan every category since the caller doesn't supply a kind hint.
	inv.RemoveFile("erigondb.toml")
	require.Empty(t, inv.MetaFiles())
}

// TestSetTorrentHash_AllBuckets is the gap-K regression guard.
// SetTorrentHash must stamp hashes on entries in every inventory bucket
// — blocks, every state domain, caplin, meta, and salt — otherwise the
// disk-scan that populateInventoryTorrentHashes runs leaves whole entry
// kinds at zero-hash, GenerateV2 silently drops them from the manifest,
// and consumers can't fetch them. The salt-missing case surfaces
// downstream as "salt not found on ReloadSalt" at exec start.
func TestSetTorrentHash_AllBuckets(t *testing.T) {
	inv := NewInventory()

	entries := []*FileEntry{
		{Name: "v1.1-000000-001000-headers.seg", FromStep: 0, ToStep: 1000, Local: true, Trust: TrustVerified}, // block
		{Domain: DomainAccounts, Kind: KindKV, Name: "v1.0-accounts.0-1024.kv", FromStep: 0, ToStep: 1024, Local: true, Trust: TrustVerified},
		{Domain: DomainReceipt, Kind: KindKV, Name: "v3.0-receipt.0-1024.kv", FromStep: 0, ToStep: 1024, Local: true, Trust: TrustVerified},
		{Kind: KindCaplin, Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Local: true, Trust: TrustVerified},
		{Kind: KindMeta, Name: "erigondb.toml", Local: true, Trust: TrustVerified},
		{Kind: KindSalt, Name: "salt-blocks.txt", Local: true, Trust: TrustVerified},
		{Kind: KindSalt, Name: "salt-state.txt", Local: true, Trust: TrustVerified},
	}
	for _, e := range entries {
		require.NoError(t, inv.AddFile(e), "AddFile %s", e.Name)
		require.Equal(t, [20]byte{}, e.TorrentHash, "%s starts with zero hash", e.Name)
	}

	for i, e := range entries {
		hash := [20]byte{byte(i + 1)}
		SetTorrentHash(inv, e.Name, hash)
	}

	// Look entries up via the inventory's public surface (not by struct
	// pointer) to confirm SetTorrentHash actually mutated the live entry.
	check := func(es []*FileEntry, name string) {
		for _, e := range es {
			if e.Name == name {
				require.NotEqual(t, [20]byte{}, e.TorrentHash, "%s hash should be stamped", name)
				require.True(t, e.Seeding, "%s Seeding should be true after stamp", name)
				return
			}
		}
		t.Fatalf("%s not found in bucket", name)
	}
	check(inv.BlockFiles(), "v1.1-000000-001000-headers.seg")
	check(inv.LocalFiles(DomainAccounts), "v1.0-accounts.0-1024.kv")
	check(inv.LocalFiles(DomainReceipt), "v3.0-receipt.0-1024.kv")
	check(inv.CaplinFiles(), "caplin/v1.1-000000-000010-beaconblocks.seg")
	check(inv.MetaFiles(), "erigondb.toml")
	check(inv.SaltFiles(), "salt-blocks.txt")
	check(inv.SaltFiles(), "salt-state.txt")
}

// TestReplaceContent_ResetsStateAndStampsHash is the cutover regression
// guard. After ReplaceContent the entry must hold the new torrent hash,
// have LifecycleState reset to Downloaded (forcing the lifecycle driver
// to re-emit Indexed → Advertisable), have Advertisable cleared, and
// a ChangeSet must fire so downstream consumers know the bytes changed.
func TestReplaceContent_ResetsStateAndStampsHash(t *testing.T) {
	inv := NewInventory()
	require.NoError(t, inv.AddFile(&FileEntry{
		Domain:       DomainAccounts,
		Kind:         KindKV,
		Name:         "v1.0-accounts.0-1024.kv",
		FromStep:     0,
		ToStep:       1024,
		TorrentHash:  [20]byte{0x11},
		Local:        true,
		Advertisable: true,
		Trust:        TrustVerified,
	}))

	// Pre-conditions: at Advertisable, hash is the minority hash.
	state, ok := inv.LifecycleState("v1.0-accounts.0-1024.kv")
	require.True(t, ok)
	require.Equal(t, LifecycleAdvertisable, state)

	sub, unsubscribe := inv.Subscribe()
	defer unsubscribe()

	canonHash := [20]byte{0xCC}
	require.True(t, ReplaceContent(inv, "v1.0-accounts.0-1024.kv", canonHash),
		"ReplaceContent must find and update the entry")

	// Post-conditions: hash swapped, state reset, Advertisable cleared.
	state, ok = inv.LifecycleState("v1.0-accounts.0-1024.kv")
	require.True(t, ok)
	require.Equal(t, LifecycleDownloaded, state,
		"State must reset to Downloaded so the driver re-evaluates")

	files := inv.LocalFiles(DomainAccounts)
	require.Len(t, files, 1)
	require.Equal(t, canonHash, files[0].TorrentHash)
	require.True(t, files[0].Seeding, "Seeding must be set after ReplaceContent")
	require.False(t, files[0].Advertisable,
		"Advertisable must clear so downstream consumers re-evaluate")

	// ChangeSet must fire so subscribers see the swap.
	select {
	case cs := <-sub:
		require.Contains(t, cs.Files, "v1.0-accounts.0-1024.kv")
	case <-time.After(time.Second):
		t.Fatal("ReplaceContent must publish a ChangeSet")
	}
}

func TestReplaceContent_MissingEntryReturnsFalse(t *testing.T) {
	inv := NewInventory()
	require.False(t, ReplaceContent(inv, "v1.0-nope.0-1024.kv", [20]byte{}),
		"ReplaceContent on an unknown name is a no-op returning false")
}

// TestInventory_DrainClearsAllCategoriesPreservesPointerAndSubscribers
// pins Drain's contract for Provider.Restart: every category-list goes
// empty, the *Inventory pointer is preserved, the subscriber channel
// stays open, and a single ChangeSet covers every removed name.
func TestInventory_DrainClearsAllCategoriesPreservesPointerAndSubscribers(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Name: "v1.1-010895-010896-headers.seg", Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{
		Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Local: true, Trust: TrustVerified,
	})
	inv.AddFile(&FileEntry{Name: "erigondb.toml", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{Name: "salt-blocks.txt", Local: true, Trust: TrustVerified})
	inv.AddFile(&FileEntry{
		Domain: DomainAccounts, FromStep: 0, ToStep: 2048,
		Name: "v1.0-accounts.0-2048.kv", Local: true, Trust: TrustVerified,
	})

	// Subscribe BEFORE Drain — must survive the drain.
	sub, unsub := inv.Subscribe()
	defer unsub()

	originalPtr := inv
	drained := inv.Drain()
	require.Same(t, originalPtr, inv, "Drain must preserve the *Inventory pointer")

	// Categories all empty.
	require.Empty(t, inv.BlockFiles(), "blocks must be empty after Drain")
	require.Empty(t, inv.CaplinFiles(), "caplin must be empty after Drain")
	require.Empty(t, inv.MetaFiles(), "meta must be empty after Drain")
	require.Empty(t, inv.SaltFiles(), "salt must be empty after Drain")
	require.Empty(t, inv.AllDomainFiles(DomainAccounts), "domain must be empty after Drain")
	require.Len(t, drained, 5, "Drain returns every removed name")

	// Subscriber sees one ChangeSet carrying every drained name.
	select {
	case cs := <-sub:
		require.ElementsMatch(t, drained, cs.Files,
			"subscriber's ChangeSet lists every drained name")
	default:
		t.Fatal("subscriber did not receive a ChangeSet")
	}

	// Re-add post-Drain to confirm the inventory is functional again.
	inv.AddFile(&FileEntry{Name: "post-drain.seg", Local: true, Trust: TrustVerified})
	require.Len(t, inv.BlockFiles(), 1, "post-Drain AddFile works normally")
}

// TestInventory_DrainOnEmptyIsNoOp confirms Drain on an empty inventory
// is a no-op — no ChangeSet fires, no panic, returns nil.
func TestInventory_DrainOnEmptyIsNoOp(t *testing.T) {
	inv := NewInventory()
	sub, unsub := inv.Subscribe()
	defer unsub()

	drained := inv.Drain()
	require.Nil(t, drained, "empty Drain returns nil")

	select {
	case <-sub:
		t.Fatal("empty Drain must not fire a ChangeSet")
	default:
	}
}
