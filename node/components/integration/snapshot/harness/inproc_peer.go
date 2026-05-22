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
	"github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// InprocPeer is the realistic-flow counterpart to FakePeer. Where FakePeer
// publishes directly onto a target node's bus, InprocPeer routes through the
// target's production sentry.Provider via AnnouncePeerManifest — exercising
// the component's event API end-to-end.
//
// The peer owns its own Inventory and deterministically assigns info-hashes
// via the same scheme FakePeer uses, registering files with the shared
// Coordinator so InprocClient can resolve them during the download path.
type InprocPeer struct {
	ID        string
	Inventory *snapshot.Inventory
	coord     *Coordinator
}

// NewInprocPeer constructs a peer with the given ID, an empty inventory, and
// a reference to the shared Coordinator.
func NewInprocPeer(id string, coord *Coordinator) *InprocPeer {
	return &InprocPeer{
		ID:        id,
		Inventory: snapshot.NewInventory(),
		coord:     coord,
	}
}

// Seed loads the fixture into the peer's inventory and registers each file's
// info-hash with the coordinator. Mirrors FakePeer.Seed so scenarios port
// over without per-file reshaping.
func (p *InprocPeer) Seed(f *Fixture) {
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

func (p *InprocPeer) assignAndRegister(e *snapshot.FileEntry) {
	if e.TorrentHash == ([20]byte{}) {
		e.TorrentHash = deterministicHash(p.ID, e.Name)
	}
	p.coord.register(e.TorrentHash, e.Name, e.Size)
}

// AnnounceTo publishes the peer's full manifest via the target's sentry
// Provider. The provider publishes flow.PeerManifestReceived on the shared
// bus, which the target's flow Orchestrator subscribes to.
func (p *InprocPeer) AnnounceTo(target *sentry.Provider) {
	domains := make(map[snapshot.Domain][]*snapshot.FileEntry)
	for _, d := range p.Inventory.Domains() {
		domains[d] = p.Inventory.AllDomainFiles(d)
	}
	target.AnnouncePeerManifest(p.ID, domains, p.Inventory.BlockFiles())
}

// Depart publishes a PeerDeparted for this peer on the target. Used by soak
// tests that churn peers to verify state cleanup.
func (p *InprocPeer) Depart(target *sentry.Provider) {
	target.AnnouncePeerDeparted(p.ID)
}
