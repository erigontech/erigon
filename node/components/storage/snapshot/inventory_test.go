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
