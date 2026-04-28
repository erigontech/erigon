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

package downloader

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// rollingTestInventory builds a tiny canonical inventory so the test
// runs in milliseconds. Two state files at canonical boundaries; their
// content is irrelevant for the publisher mechanics — what matters is
// that GenerateV2 produces non-empty output that round-trips.
func rollingTestInventory(_ *testing.T, marker byte) *snapshotinv.Inventory {
	inv := snapshotinv.NewInventory()
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainAccounts,
		FromStep:    0,
		ToStep:      1024,
		Name:        "v1.0-accounts.0-1024.kv",
		TorrentHash: [20]byte{marker, 0xaa},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	inv.AddFile(&snapshotinv.FileEntry{
		Domain:      snapshotinv.DomainStorage,
		FromStep:    0,
		ToStep:      1024,
		Name:        "v1.0-storage.0-1024.kv",
		TorrentHash: [20]byte{marker, 0xbb},
		Local:       true,
		Trust:       snapshotinv.TrustVerified,
	})
	return inv
}

// listV2Generations returns sorted seqs of chain.v2.<seq>.toml files in
// snapDir.
func listV2Generations(t *testing.T, snapDir string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	var seqs []uint64
	for _, e := range entries {
		if seq, ok := ParseChainTomlV2FileName(e.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

// TestRollingV2Publisher_GenerationsAdvance covers the basic shape:
// repeated Publish calls produce sequentially-numbered files and
// distinct infohashes (since the metainfo Name field changes each
// time).
func TestRollingV2Publisher_GenerationsAdvance(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)
	require.Empty(t, pub.History())

	inv := rollingTestInventory(t, 0x10)

	hashes := make([]metainfo.Hash, 5)
	for i := range hashes {
		h, err := pub.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err, "publish %d", i)
		require.NotEqual(t, metainfo.Hash{}, h, "publish %d: zero hash", i)
		hashes[i] = h
	}

	require.Equal(t, []uint64{0, 1, 2, 3, 4}, pub.History())
	require.Equal(t, []uint64{0, 1, 2, 3, 4}, listV2Generations(t, snapDir))
	for i := 1; i < len(hashes); i++ {
		require.NotEqual(t, hashes[i-1], hashes[i],
			"publish %d shares infohash with %d — Name field must differ", i, i-1)
	}
}

// TestRollingV2Publisher_RollingBufferEvicts caps history at the
// configured max and removes files for evicted generations.
func TestRollingV2Publisher_RollingBufferEvicts(t *testing.T) {
	snapDir := t.TempDir()
	const cap = 3
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, cap)
	require.NoError(t, err)

	inv := rollingTestInventory(t, 0x20)
	for i := 0; i < 7; i++ {
		_, err := pub.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err, "publish %d", i)
	}

	// History contains only the most recent `cap` generations.
	require.Equal(t, []uint64{4, 5, 6}, pub.History())

	// On disk: only those generations remain. Older files (and their
	// .torrent sidecars) are gone.
	require.Equal(t, []uint64{4, 5, 6}, listV2Generations(t, snapDir))
	for _, oldSeq := range []uint64{0, 1, 2, 3} {
		oldName := ChainTomlV2FileNameForSeq(oldSeq)
		_, err := os.Stat(filepath.Join(snapDir, oldName))
		require.True(t, os.IsNotExist(err), "%s should have been evicted", oldName)
		_, err = os.Stat(filepath.Join(snapDir, oldName+".torrent"))
		require.True(t, os.IsNotExist(err), "%s.torrent should have been evicted", oldName)
	}
}

// TestRollingV2Publisher_RestartResumesSeq covers the discovery path:
// a fresh publisher built against a snapDir that already contains
// generations resumes seq numbering from max(existing)+1.
func TestRollingV2Publisher_RestartResumesSeq(t *testing.T) {
	snapDir := t.TempDir()
	inv := rollingTestInventory(t, 0x30)

	// Publish a few generations, then drop the publisher.
	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := first.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err)
	}
	require.Equal(t, []uint64{0, 1, 2}, first.History())

	// New publisher discovers existing files, resumes from seq 3.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2}, second.History())

	_, err = second.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, second.History())
	require.Equal(t, []uint64{0, 1, 2, 3}, listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_RestartShrinksCap covers the case where a
// previous run wrote more generations than the new cap allows. The new
// publisher discovers all of them, then evicts on the next Publish.
func TestRollingV2Publisher_RestartShrinksCap(t *testing.T) {
	snapDir := t.TempDir()
	inv := rollingTestInventory(t, 0x40)

	// First run — cap of 10, publish 6 generations.
	first, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 10)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		_, err := first.Publish(context.Background(), inv, 0, nil)
		require.NoError(t, err)
	}
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5}, first.History())

	// Restart with a smaller cap — discovery sees all 6.
	second, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5}, second.History())

	// Next Publish triggers eviction down to 3 (counting the new entry).
	_, err = second.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{4, 5, 6}, second.History())
	require.Equal(t, []uint64{4, 5, 6}, listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_CleanupOrphans drops chain.v2.<seq>.toml files
// whose seq isn't in history — simulating a crash mid-publish that
// wrote files but never advanced the in-memory state.
func TestRollingV2Publisher_CleanupOrphans(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	inv := rollingTestInventory(t, 0x50)

	// Publish two generations normally.
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, pub.History())

	// Plant orphan files on disk (no .torrent sidecar, simulating a
	// crash partway through Publish).
	orphanName := ChainTomlV2FileNameForSeq(99)
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, orphanName), []byte("stub"), 0o644))

	// Cleanup removes the orphan, leaves history intact.
	require.NoError(t, pub.Cleanup())
	require.Equal(t, []uint64{0, 1}, pub.History())
	require.Equal(t, []uint64{0, 1}, listV2Generations(t, snapDir))
}

// TestRollingV2Publisher_ENRUpdaterFires confirms enrUpdater is called
// once per Publish with the correct fields. Belt-and-braces against
// the auto-publisher integration regressing.
func TestRollingV2Publisher_ENRUpdaterFires(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil, 0)
	require.NoError(t, err)

	inv := rollingTestInventory(t, 0x60)

	var calls int
	var lastCT enr.ChainToml
	enrUpdater := func(ct enr.ChainToml) {
		calls++
		lastCT = ct
	}
	hash, err := pub.Publish(context.Background(), inv, 12345, enrUpdater)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, [20]byte(hash), lastCT.InfoHash)
	require.Equal(t, uint64(12345), lastCT.AuthoritativeBlocks)
	require.Equal(t, uint64(12345), lastCT.KnownBlocks)

	// Nil updater: tolerated.
	_, err = pub.Publish(context.Background(), inv, 0, nil)
	require.NoError(t, err)
}

// TestParseChainTomlV2FileName covers the predicate's accept / reject
// behaviour. Lock the wire format down so a typo elsewhere can't widen it.
func TestParseChainTomlV2FileName(t *testing.T) {
	cases := []struct {
		name    string
		wantOK  bool
		wantSeq uint64
	}{
		{"chain.v2.0.toml", true, 0},
		{"chain.v2.42.toml", true, 42},
		{"chain.v2.18446744073709551614.toml", true, 18446744073709551614},
		// Reject everything else — no leading dot, no v1, no missing seq, etc.
		{"chain.v2.toml", false, 0},
		{"chain.v2.0", false, 0},
		{"chain.v2.0.tml", false, 0},
		{"chain.v3.0.toml", false, 0},
		{"chain.v2.-1.toml", false, 0},
		{"chain.v2.0.toml.bak", false, 0},
		{"prefix.chain.v2.0.toml", false, 0},
		{"chain.v2.0.toml.torrent", false, 0},
	}
	for _, c := range cases {
		seq, ok := ParseChainTomlV2FileName(c.name)
		require.Equal(t, c.wantOK, ok, "ok mismatch for %q", c.name)
		if c.wantOK {
			require.Equal(t, c.wantSeq, seq, "seq mismatch for %q", c.name)
		}
	}
}
