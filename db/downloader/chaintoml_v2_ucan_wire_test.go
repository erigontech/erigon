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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/p2p/enr"
)

// TestRollingV2Publisher_ContentUCANRepublishStable pins the fix for
// the leg-M cycle 7 bug: MintContentUCAN embeds time.Now() and a
// randomised signature, so re-minting the SAME genID's Content UCAN
// produces different bytes each call. If publishOne re-mints on
// republish and overwrites the .ucan file, BuildTorrentIfNeed's
// exists-short-circuit leaves the .torrent pointing at the previous
// generation's piece hash — consumers download the current .ucan
// bytes over BT, verify against the stale piece hash, mismatch,
// retry forever ("downloading X: context deadline exceeded" in
// consumer's UCAN gate).
//
// Fix: on republish of a genID whose .ucan + .torrent already exist,
// skip the mint entirely — reuse the on-disk pair.
func TestRollingV2Publisher_ContentUCANRepublishStable(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	// Minter returning DIFFERENT bytes each call — simulating
	// MintContentUCAN's non-determinism (time.Now() + random signature).
	var mintCallCount int
	pub.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
		mintCallCount++
		out := []byte("content-ucan-attestation-bytes-call-")
		out = append(out, byte('0'+mintCallCount))
		return out, nil
	})

	inv := rollingTestInventory(t, 0x70)
	updater := func(ct enr.ChainToml) {}

	// Publish 1: mint fires, .ucan and .torrent land.
	hash1, err := pub.Publish(context.Background(), inv, 12345, updater)
	require.NoError(t, err)
	require.Equal(t, 1, mintCallCount)

	// Locate the .ucan and .torrent so we can byte-check them.
	var ucanPath, ucanTorrentPath string
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".ucan" {
			ucanPath = filepath.Join(snapDir, e.Name())
			ucanTorrentPath = ucanPath + ".torrent"
			break
		}
	}
	require.NotEmpty(t, ucanPath, "publish must produce a .ucan file")
	ucanBytes1, err := os.ReadFile(ucanPath)
	require.NoError(t, err)
	torrentBytes1, err := os.ReadFile(ucanTorrentPath)
	require.NoError(t, err)

	// Publish 2: same inventory, same genID. The mint MUST NOT run
	// again — otherwise it overwrites the .ucan with different bytes
	// while the .torrent stays at the piece hash of the first mint.
	hash2, err := pub.Publish(context.Background(), inv, 12345, updater)
	require.NoError(t, err)
	require.Equal(t, hash1, hash2, "same inventory → same manifest hash")
	require.Equal(t, 1, mintCallCount,
		"republish of same genID must reuse the existing .ucan; re-minting would desync the .ucan bytes from the .torrent piece hash")

	ucanBytes2, err := os.ReadFile(ucanPath)
	require.NoError(t, err)
	torrentBytes2, err := os.ReadFile(ucanTorrentPath)
	require.NoError(t, err)

	require.Equal(t, ucanBytes1, ucanBytes2,
		".ucan bytes must be byte-identical across republishes of the same genID")
	require.Equal(t, torrentBytes1, torrentBytes2,
		".torrent bytes must be byte-identical across republishes of the same genID")
}

// TestSetENRUpdater_CachesLastEmittedCT pins the SetENRUpdater shim:
// every enrUpdater invocation updates d.lastEmittedENRChainToml
// before delegating to the caller's fn. The defensive re-assert path
// reads this cache to re-emit verbatim.
func TestSetENRUpdater_CachesLastEmittedCT(t *testing.T) {
	d := &Downloader{}
	var callerCalls int
	d.SetENRUpdater(func(ct enr.ChainToml) {
		callerCalls++
	})

	want := enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            [20]byte{1, 2, 3},
		DomainSteps:         42,
		MergeDepth:          17,
		ContentUCANHash:     [20]byte{9, 9, 9},
		V2InfoHash:          [20]byte{1, 2, 3},
		MinStep:             5,
	}
	d.enrUpdater(want)

	require.Equal(t, 1, callerCalls, "caller fn must fire exactly once")
	d.lock.RLock()
	got := d.lastEmittedENRChainToml
	d.lock.RUnlock()
	require.Equal(t, want, got, "shim must cache the exact struct emitted")
}

// TestSetENRUpdater_NilClears pins the nil-safe path.
func TestSetENRUpdater_NilClears(t *testing.T) {
	d := &Downloader{}
	d.SetENRUpdater(func(ct enr.ChainToml) {})
	require.NotNil(t, d.enrUpdater)
	d.SetENRUpdater(nil)
	require.Nil(t, d.enrUpdater)
}

// TestRollingV2Publisher_MinterSetBetweenPublishes covers the ordering
// concern from leg-M cycle 6: publisher.Publish is called once BEFORE
// contentMinter is wired (CU=0 in captured ENR), the minter is wired,
// then Publish is called again with the same inventory. The second
// Publish MUST re-emit and stamp the ENR with a non-zero CU hash.
//
// If Publish is a no-op for byte-identical content, the ENR stays at
// CU=0 forever even though .ucan files are being produced (they'd be
// produced by SUBSEQUENT content-changing publishes, but the ENR
// update at those calls happens AFTER the mint, so CU should always
// match what the current call minted — meaning if this test passes,
// the bug is elsewhere).
func TestRollingV2Publisher_MinterSetBetweenPublishes(t *testing.T) {
	snapDir := t.TempDir()
	pub, err := NewRollingV2Publisher(snapDir, NewAtomicTorrentFS(snapDir), nil)
	require.NoError(t, err)
	pub.SetENRFingerprint(testENRFP)

	inv := rollingTestInventory(t, 0x70)

	var lastCT enr.ChainToml
	var enrCalls int
	updater := func(ct enr.ChainToml) {
		enrCalls++
		lastCT = ct
	}

	// Publish 1: no minter. CU should be zero.
	_, err = pub.Publish(context.Background(), inv, 12345, updater)
	require.NoError(t, err)
	require.Equal(t, 1, enrCalls, "first publish must fire ENR updater")
	require.Equal(t, [20]byte{}, lastCT.ContentUCANHash,
		"no minter → CU=zero (baseline)")

	// Wire minter + delegation between publishes.
	pub.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
		return []byte("content-ucan-attestation-stub"), nil
	})

	// Publish 2: same inventory, minter now wired. What happens?
	// If Publish is a no-op on unchanged content, ENR stays at CU=0.
	// If Publish always emits + fires ENR, CU becomes non-zero.
	_, err = pub.Publish(context.Background(), inv, 12345, updater)
	require.NoError(t, err)
	require.Equal(t, 2, enrCalls,
		"second publish must fire ENR updater even on unchanged inventory (else the CU stamp from a minter wired AFTER the first publish never reaches the ENR)")
	require.NotEqual(t, [20]byte{}, lastCT.ContentUCANHash,
		"second publish with wired minter must produce non-zero CU in the ENR")
}
