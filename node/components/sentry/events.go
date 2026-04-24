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

package sentry

import "github.com/erigontech/erigon/p2p/enode"

// PeerConnected fires when the P2P layer completes a handshake with a new
// peer. Carries the full resolved enode so subscribers can read ENR entries
// (e.g. the chain-toml advertisement) without re-resolving.
//
// This is the production signal the manifest-exchange component subscribes
// to in order to drive V2 chain.toml fetches. Test harnesses can drive the
// same event via Provider.PublishPeerConnected.
type PeerConnected struct {
	Peer *enode.Node
}

// PeerDisconnected fires when a previously-connected peer goes away.
// PeerID matches what PeerConnected would have carried via Peer.ID().String().
type PeerDisconnected struct {
	PeerID string
}
