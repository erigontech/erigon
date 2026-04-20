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
	"crypto/sha1" //nolint:gosec // info-hash placeholder; not a cryptographic primitive
	"fmt"

	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// FakePeer simulates a remote peer that advertises an inventory to a target
// node and serves its files through a shared Coordinator. Production replaces
// this with the Downloader component bridging a real torrent swarm.
//
// FakePeer does not own a Node — it publishes onto a target's bus. A single
// peer can announce to multiple targets by calling AnnounceTo repeatedly.
type FakePeer struct {
	ID        string
	Inventory *snapshot.Inventory
	coord     *Coordinator
}

// NewFakePeer constructs a peer with the given ID and an empty inventory.
// The peer shares the coordinator with every other transport/peer in the
// scenario so file transfer resolves in-process.
func NewFakePeer(id string, coord *Coordinator) *FakePeer {
	return &FakePeer{
		ID:        id,
		Inventory: snapshot.NewInventory(),
		coord:     coord,
	}
}

// Seed loads the given fixture into the peer's inventory, auto-assigning a
// deterministic info-hash per file name and registering synthetic bytes with
// the coordinator.
func (p *FakePeer) Seed(f *Fixture) {
	f.Apply(p.Inventory)

	for _, d := range p.Inventory.Domains() {
		for _, e := range p.Inventory.AllDomainFiles(d) {
			p.assignAndRegister(e)
		}
	}
	for _, e := range p.Inventory.BlockFiles() {
		p.assignAndRegister(e)
	}
}

func (p *FakePeer) assignAndRegister(e *snapshot.FileEntry) {
	if e.TorrentHash == ([20]byte{}) {
		e.TorrentHash = deterministicHash(p.ID, e.Name)
	}
	p.coord.register(e.TorrentHash, e.Name, syntheticBytes(e.Size))
}

// AnnounceTo publishes a PeerManifestReceived on the target node's bus,
// advertising every file in the peer's inventory.
func (p *FakePeer) AnnounceTo(target *Node) {
	domains := make(map[snapshot.Domain][]*snapshot.FileEntry)
	for _, d := range p.Inventory.Domains() {
		domains[d] = p.Inventory.AllDomainFiles(d)
	}
	target.Bus.Publish(flow.PeerManifestReceived{
		PeerID:  p.ID,
		Domains: domains,
		Blocks:  p.Inventory.BlockFiles(),
	})
}

// deterministicHash makes a stable fake info-hash from (peer ID, file name).
// Collisions are vanishingly unlikely within a test's namespace. The hash
// function choice is irrelevant since SimulatedTransport looks it up in a
// map — no cryptographic properties are relied upon.
func deterministicHash(peerID, fileName string) [20]byte {
	return sha1.Sum([]byte(fmt.Sprintf("%s:%s", peerID, fileName))) //nolint:gosec
}

// syntheticBytes fills a slice of the requested size with a recognisable A–Z
// pattern so a hex-dump during debugging visibly distinguishes fixture bytes
// from uninitialised or zeroed memory.
func syntheticBytes(size int64) []byte {
	if size <= 0 {
		return nil
	}
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('A' + (i % 26))
	}
	return b
}
