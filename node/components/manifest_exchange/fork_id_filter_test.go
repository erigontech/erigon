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

package manifest_exchange

import (
	"encoding/hex"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/p2p/forkid"
)

// hexGenesisFork returns the same hex(CRC32(genesis_hash)) that
// downloader.BuildChainIdentity produces — the manifest's GenesisFork
// scalar.
func hexGenesisFork(h common.Hash) string {
	b := forkid.ChecksumToBytes(crc32.ChecksumIEEE(h[:]))
	return hex.EncodeToString(b[:])
}

// localFixture: a chain at genesisHash{0x01} with two height forks
// (100, 200) and two time forks (1700000000, 1800000000). This is the
// reference "local node" used across the filter tests.
func localFixture() (string, common.Hash, []uint64, []uint64) {
	h := common.Hash{0x01}
	return hexGenesisFork(h), h, []uint64{100, 200}, []uint64{1700000000, 1800000000}
}

// TestForkIDFilter_EmptyGenesisFork: a manifest without identity (a
// publisher that predates Phase 1) must be accepted — back-compat.
func TestForkIDFilter_EmptyGenesisFork(t *testing.T) {
	gf, gh, h, ti := localFixture()
	filter := BuildForkIDFilter(gf, gh, h, ti)
	require.NoError(t, filter(&downloader.ChainTomlV2{GenesisFork: ""}))
}

// TestForkIDFilter_DifferentGenesis: a peer on a different chain
// (different genesis hash → different GenesisFork) must be rejected
// before any fork-list comparison.
func TestForkIDFilter_DifferentGenesis(t *testing.T) {
	gf, gh, h, ti := localFixture()
	filter := BuildForkIDFilter(gf, gh, h, ti)
	other := hexGenesisFork(common.Hash{0xff})
	err := filter(&downloader.ChainTomlV2{GenesisFork: other})
	require.ErrorContains(t, err, "genesis-fork mismatch")
}

// TestForkIDFilter_SameLineage: a peer on the same chain with the
// same fork schedule is accepted.
func TestForkIDFilter_SameLineage(t *testing.T) {
	gf, gh, h, ti := localFixture()
	filter := BuildForkIDFilter(gf, gh, h, ti)
	peer := &downloader.ChainTomlV2{
		GenesisFork: gf,
		Forks: []downloader.ForkActivation{
			{Block: 100}, {Block: 200},
			{Time: 1700000000}, {Time: 1800000000},
		},
	}
	require.NoError(t, filter(peer))
}

// TestForkIDFilter_PeerOlderRelease: a peer at an older release
// (subset of local's forks — same hashes for the activations it has
// applied) is accepted. This is the snapshot-loose case the standard
// EIP-2124 filter rejects with ErrRemoteStale.
func TestForkIDFilter_PeerOlderRelease(t *testing.T) {
	gf, gh, h, ti := localFixture()
	filter := BuildForkIDFilter(gf, gh, h, ti)
	peer := &downloader.ChainTomlV2{
		GenesisFork: gf,
		Forks:       []downloader.ForkActivation{{Block: 100}}, // only first fork
	}
	require.NoError(t, filter(peer), "older-release peer (subset of forks) must be accepted")
}

// TestForkIDFilter_PeerNewerRelease: a peer at a newer release
// (superset — applies an additional fork local doesn't know about) is
// accepted. Its hash chain extends local's.
func TestForkIDFilter_PeerNewerRelease(t *testing.T) {
	gf, gh, h, ti := localFixture()
	filter := BuildForkIDFilter(gf, gh, h, ti)
	peer := &downloader.ChainTomlV2{
		GenesisFork: gf,
		Forks: []downloader.ForkActivation{
			{Block: 100}, {Block: 200},
			{Time: 1700000000}, {Time: 1800000000},
			{Time: 1900000000}, // future fork local doesn't have
		},
	}
	require.NoError(t, filter(peer), "newer-release peer (superset of forks) must be accepted")
}

// TestForkIDFilter_DivergentLineage: shared genesis but a fork at a
// DIFFERENT activation than local — the hash chains diverge after the
// first divergent point, and the peer's final hash appears nowhere in
// local's chain. Rejected.
func TestForkIDFilter_DivergentLineage(t *testing.T) {
	gf, gh, h, ti := localFixture()
	filter := BuildForkIDFilter(gf, gh, h, ti)
	peer := &downloader.ChainTomlV2{
		GenesisFork: gf,
		Forks: []downloader.ForkActivation{
			{Block: 150}, // local has 100; this is a different lineage
			{Block: 200},
		},
	}
	err := filter(peer)
	require.ErrorContains(t, err, "lineage incompatible")
}
