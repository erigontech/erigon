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
	"fmt"
	"path/filepath"

	"github.com/anacrolix/torrent/metainfo"

	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// ChainTomlV2FileName is the on-disk name of the V2 manifest. It lives
// alongside V1 (`chain.toml`) so peers on either format coexist during
// rollout.
const ChainTomlV2FileName = "chain.toml.v2"

// ChainTomlV2Path returns the full path to chain.toml.v2 in snapDir.
func ChainTomlV2Path(snapDir string) string {
	return filepath.Join(snapDir, ChainTomlV2FileName)
}

// SaveChainTomlV2 atomically writes the V2 manifest bytes to snapDir.
func SaveChainTomlV2(snapDir string, tomlBytes []byte) error {
	return saveChainTomlFile(ChainTomlV2Path(snapDir), tomlBytes)
}

// BuildChainTomlV2Torrent creates (or recreates) the .torrent file for
// chain.toml.v2 and returns its info-hash.
func BuildChainTomlV2Torrent(snapDir string, torrentFS *AtomicTorrentFS) (metainfo.Hash, error) {
	return buildChainTomlTorrentByName(ChainTomlV2FileName, snapDir, torrentFS)
}

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

// PublishChainTomlV2 orchestrates the full V2 publish flow from an
// inventory:
//
//  1. GenerateV2 from inventory
//  2. MarshalV2 to deterministic TOML bytes
//  3. SaveChainTomlV2 to disk
//  4. BuildChainTomlV2Torrent to compute the infohash
//  5. Call enrUpdater with the V2 infohash + DomainSteps + MergeDepth
//
// authoritativeBlocks is the caller's declaration of how many blocks this
// node considers authoritative (typically snapcfg.KnownCfg.ExpectBlocks).
// Tests may pass 0 when not exercising block coverage.
//
// enrUpdater may be nil — callers that don't have a live P2P node yet
// (e.g. cold-start before DevP2P is up) get the file written and the
// torrent built but the ENR update skipped. The caller is expected to
// rerun Publish (or trigger the ENR update separately) once P2P comes up.
//
// Returns the V2 infohash so callers can log / verify it.
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

	manifest := GenerateV2(inv)

	tomlBytes, err := MarshalV2(manifest)
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("marshal chain.toml.v2: %w", err)
	}

	if err := SaveChainTomlV2(snapDir, tomlBytes); err != nil {
		return metainfo.Hash{}, fmt.Errorf("save chain.toml.v2: %w", err)
	}

	infoHash, err := BuildChainTomlV2Torrent(snapDir, torrentFS)
	if err != nil {
		return metainfo.Hash{}, err
	}

	if enrUpdater != nil {
		domainSteps, mergeDepth := ComputeENRFields(manifest)
		enrUpdater(enr.ChainToml{
			AuthoritativeBlocks: authoritativeBlocks,
			KnownBlocks:         authoritativeBlocks,
			InfoHash:            infoHash,
			DomainSteps:         domainSteps,
			MergeDepth:          mergeDepth,
		})
	}

	return infoHash, nil
}
