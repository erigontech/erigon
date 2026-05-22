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
	"fmt"
	"hash/crc32"
	"slices"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/p2p/forkid"
)

// BuildForkIDFilter constructs a ForkIDFilterFn from the local node's
// chain configuration. The returned filter is the consumer-side
// EIP-2124 compatibility gate (fork-spec.md § Identification):
//
//   - Manifests whose GenesisFork is empty are accepted (Phase-1
//     back-compat for publishers that predate chain-identity).
//   - A GenesisFork that doesn't match the local one is rejected as a
//     definite cross-chain manifest.
//   - Otherwise the peer's FORK_HASH chain is computed from its
//     [[forks]] list and compared against the local chain's. The
//     manifest is accepted when one chain's final hash appears in the
//     other (the "common prefix" relation): peer is a subset, an
//     equal, or a superset of the local lineage. This is the
//     fork-spec "snapshot-loose" rule — FORK_NEXT-based rejections of
//     the devp2p filter are intentionally not applied, because for
//     immutable historical data only FORK_HASH lineage matters and a
//     peer at an older release holds byte-identical pre-upgrade data.
//
// localHeightForks and localTimeForks come from forkid.GatherForks for
// the local chain config. localGenesisFork is the hex CRC32 of the
// local genesis hash (downloader.BuildChainIdentity's first return).
func BuildForkIDFilter(
	localGenesisFork string,
	localGenesisHash common.Hash,
	localHeightForks, localTimeForks []uint64,
) ForkIDFilterFn {
	localChain := forkHashChain(localGenesisHash, localHeightForks, localTimeForks)
	return func(m *downloader.ChainTomlV2) error {
		if m.GenesisFork == "" {
			return nil
		}
		if !strings.EqualFold(m.GenesisFork, localGenesisFork) {
			return fmt.Errorf("genesis-fork mismatch: peer=%q local=%q", m.GenesisFork, localGenesisFork)
		}
		peerHeights, peerTimes := splitActivations(m.Forks)
		// Both sides start at the same genesis-fork (verified above),
		// so the peer's chain is computed against the local genesis.
		peerChain := forkHashChain(localGenesisHash, peerHeights, peerTimes)
		if !lineageCompatible(localChain, peerChain) {
			return fmt.Errorf("fork-ID lineage incompatible: peer chain not on the local lineage")
		}
		return nil
	}
}

// splitActivations partitions a manifest's forks into ascending
// height-fork and time-fork slices for hash-chain construction.
func splitActivations(forks []downloader.ForkActivation) (heights, times []uint64) {
	for _, f := range forks {
		switch {
		case f.Block != 0:
			heights = append(heights, f.Block)
		case f.Time != 0:
			times = append(times, f.Time)
		}
	}
	slices.Sort(heights)
	slices.Sort(times)
	return heights, times
}

// forkHashChain computes the EIP-2124 FORK_HASH chain for a genesis +
// fork schedule: [CRC32(genesis), CRC32(genesis ‖ h1), CRC32(genesis ‖
// h1 ‖ h2), …]. Each prefix hash corresponds to "having applied the
// first k forks."
//
// Height forks come first, then time forks — matching forkid.GatherForks
// and forkid.newFilter, so the chain a peer running standard Erigon
// would produce matches exactly.
func forkHashChain(genesisHash common.Hash, heights, times []uint64) [][4]byte {
	h := crc32.ChecksumIEEE(genesisHash[:])
	out := make([][4]byte, 0, 1+len(heights)+len(times))
	out = append(out, forkid.ChecksumToBytes(h))
	for _, f := range heights {
		h = forkid.ChecksumUpdate(h, f)
		out = append(out, forkid.ChecksumToBytes(h))
	}
	for _, f := range times {
		h = forkid.ChecksumUpdate(h, f)
		out = append(out, forkid.ChecksumToBytes(h))
	}
	return out
}

// lineageCompatible reports whether two FORK_HASH chains share a
// common-prefix relation — either one's final hash appears in the
// other. This is the "snapshot-loose" compatibility: peer is a subset,
// an equal, or a superset of the local lineage. Two peers on the same
// chain at different release versions remain compatible.
func lineageCompatible(localChain, peerChain [][4]byte) bool {
	if len(localChain) == 0 || len(peerChain) == 0 {
		return false
	}
	localFinal := localChain[len(localChain)-1]
	peerFinal := peerChain[len(peerChain)-1]
	for _, h := range localChain {
		if h == peerFinal {
			return true
		}
	}
	for _, h := range peerChain {
		if h == localFinal {
			return true
		}
	}
	return false
}
