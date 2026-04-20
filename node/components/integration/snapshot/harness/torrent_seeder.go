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

package harness

import (
	"fmt"

	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// SeedFixtureAsTorrentPeer writes the Fixture's file entries to disk via the
// given TorrentTransport, registers each with the torrent client, and adds
// them to inv with the real info-hash returned by the client.
//
// Per-file synthetic bytes are generated from a stable (fileName, size)
// seed — two peers seeding the same fixture produce byte-identical files
// and therefore identical info-hashes, which is what convergent-merge
// peer scenarios need. Bytes are a repeating pattern so a downloaded file
// can be identified in a hex dump.
//
// Callers typically follow with BuildPeerManifest(id, inv) to publish the
// manifest on a leecher's bus.
func SeedFixtureAsTorrentPeer(transport *TorrentTransport, inv *snapshot.Inventory, f *Fixture) error {
	seed := func(e *snapshot.FileEntry) error {
		data := fixtureFileBytes(e.Name, e.Size)
		hash, err := transport.Seed(e.Name, data)
		if err != nil {
			return fmt.Errorf("seed %s: %w", e.Name, err)
		}
		e.TorrentHash = hash
		e.Local = true
		e.Trust = snapshot.TrustVerified
		e.Size = int64(len(data))
		inv.AddFile(e)
		return nil
	}
	for _, entries := range f.Domains {
		for _, e := range entries {
			if err := seed(e); err != nil {
				return err
			}
		}
	}
	for _, e := range f.Blocks {
		if err := seed(e); err != nil {
			return err
		}
	}
	return nil
}

// BuildPeerManifest assembles the PeerManifestReceived payload for a peer
// whose inventory has been populated by SeedFixtureAsTorrentPeer or
// equivalent.
func BuildPeerManifest(peerID string, inv *snapshot.Inventory) flow.PeerManifestReceived {
	domains := make(map[snapshot.Domain][]*snapshot.FileEntry)
	for _, d := range inv.Domains() {
		domains[d] = inv.AllDomainFiles(d)
	}
	return flow.PeerManifestReceived{
		PeerID:  peerID,
		Domains: domains,
		Blocks:  inv.BlockFiles(),
	}
}

// fixtureFileBytes produces a deterministic byte slice from (name, size).
// Independent transports seeding the same fixture produce identical bytes
// — essential when two peers claim to serve the same file and a leecher
// uses the shared info-hash to fetch from whichever peer it finds first.
func fixtureFileBytes(name string, size int64) []byte {
	if size <= 0 {
		size = 256
	}
	b := make([]byte, size)
	seed := byte(0)
	for i := range name {
		seed ^= name[i]
	}
	for i := range b {
		b[i] = seed + byte(i%26)
	}
	return b
}
