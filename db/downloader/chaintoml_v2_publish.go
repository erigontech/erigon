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
	"fmt"

	"github.com/anacrolix/torrent/metainfo"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// ComputeENRFields derives the DomainSteps and MergeDepth values the ENR
// advertises from a V2 manifest. See chaintoml-v2-spec-baseline §4.2:
//
//   - DomainSteps: the largest Coverage[1] across all domains. Zero if
//     there are no domains.
//   - MergeDepth: the size in steps of the largest canonical file listed.
//     Zero if there are no canonical files.
func ComputeENRFields(manifest *ChainTomlV2) (domainSteps, mergeDepth uint64) {
	if manifest == nil {
		return 0, 0
	}
	for _, dm := range manifest.Domains {
		if dm == nil {
			continue
		}
		if dm.Coverage[1] > domainSteps {
			domainSteps = dm.Coverage[1]
		}
		for _, f := range dm.Files {
			size := f.Range[1] - f.Range[0]
			if size > mergeDepth {
				mergeDepth = size
			}
		}
	}
	return domainSteps, mergeDepth
}

// PublishChainTomlV2 is a one-shot wrapper around RollingV2Publisher
// for callers that don't keep a long-lived publisher (tests, simple
// startup paths). It writes the next chain.v2.<seq>.toml generation
// based on what's already on disk, builds the .torrent, registers it
// with no torrent client (caller's responsibility for one-shot use),
// and calls enrUpdater with the new info-hash.
//
// For repeated publication driven by inventory growth, callers should
// hold a long-lived RollingV2Publisher instead — its rolling buffer
// keeps recent generations seedable, which a fresh wrapper can't do
// because each invocation rebuilds the publisher state from disk.
//
// authoritativeBlocks passes through to the ENR ChainToml entry.
// enrUpdater may be nil for cold-start paths where P2P isn't up yet;
// the manifest still gets written and the .torrent still gets built.
//
// Returns the V2 infohash for logs / verification.
func PublishChainTomlV2(
	snapDir string,
	torrentFS *AtomicTorrentFS,
	inv *snapshotinv.Inventory,
	authoritativeBlocks uint64,
	enrUpdater func(enr.ChainToml),
) (metainfo.Hash, error) {
	if inv == nil {
		return metainfo.Hash{}, fmt.Errorf("PublishChainTomlV2: nil inventory")
	}
	if torrentFS == nil {
		return metainfo.Hash{}, fmt.Errorf("PublishChainTomlV2: nil torrent fs")
	}

	pub, err := NewRollingV2Publisher(snapDir, torrentFS, nil, 0)
	if err != nil {
		return metainfo.Hash{}, err
	}
	return pub.Publish(context.Background(), inv, authoritativeBlocks, enrUpdater)
}
