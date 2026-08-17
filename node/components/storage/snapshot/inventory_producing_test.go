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

// RemoveFile from Inventory records the name as "being produced locally"
// so downstream coordinators (the download orchestrator) can filter it
// out of peer manifest re-requests until we re-add the file. AddFile
// clears the producing mark for that name — the file is back in the
// authoritative set. Fixes the retire-drop → peer-manifest-redownload
// race documented in checkpoint-2026-08-17-retire-race-fix-verified.
func TestInventory_ProducingLifecycle(t *testing.T) {
	inv := NewInventory()
	const name = "v2.2-commitment.310-311.kv"

	require.False(t, inv.IsProducing(name), "fresh inventory: nothing is producing")

	inv.AddFile(&FileEntry{
		Domain:   DomainCommitment,
		FromStep: 310, ToStep: 311,
		Name:  name,
		Kind:  KindKV,
		Local: true,
	})
	require.False(t, inv.IsProducing(name), "AddFile does not mark producing")

	inv.RemoveFile(name)
	require.True(t, inv.IsProducing(name), "RemoveFile marks producing so downloads defer to local re-production")

	inv.AddFile(&FileEntry{
		Domain:   DomainCommitment,
		FromStep: 310, ToStep: 311,
		Name:  name,
		Kind:  KindKV,
		Local: true,
	})
	require.False(t, inv.IsProducing(name), "AddFile of a producing name clears the mark")
}

// Producing is only set by RemoveFile, not by ReplaceWithMerge — a
// merge that supersedes a smaller file is not "re-producing" the smaller
// file, it's producing a wider replacement. The smaller file's slot is
// gone from the authoritative set but not scheduled for local rebuild.
func TestInventory_ReplaceWithMergeDoesNotMarkProducing(t *testing.T) {
	inv := NewInventory()
	small := "v2.2-commitment.310-311.kv"
	wide := "v2.2-commitment.308-311.kv"
	inv.AddFile(&FileEntry{
		Domain:   DomainCommitment,
		FromStep: 310, ToStep: 311,
		Name:  small,
		Kind:  KindKV,
		Local: true,
	})

	merged := &FileEntry{
		Domain:   DomainCommitment,
		FromStep: 308, ToStep: 311,
		Name:  wide,
		Kind:  KindKV,
		Local: true,
	}
	inv.ReplaceWithMerge(merged, []string{small})

	require.False(t, inv.IsProducing(small), "ReplaceWithMerge does not schedule local rebuild of the replaced file")
	require.False(t, inv.IsProducing(wide), "the merged file was just added — not producing")
}
