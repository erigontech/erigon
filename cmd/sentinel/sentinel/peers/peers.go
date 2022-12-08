/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package peers

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxBadPeers     = 1000 // Always cap memory consumption at 1 MB
	DefaultMaxPeers = 33
	MaxBadResponses = 10
)

type Peers struct {
	badPeers  *lru.Cache // Keep track of bad peers
	penalties *lru.Cache // Keep track on how many penalties a peer accumulated, PeerId => penalties
	host      host.Host
	mu        sync.Mutex
}

func New(host host.Host) *Peers {
	badPeers, err := lru.New(maxBadPeers)
	if err != nil {
		panic(err)
	}

	penalties, err := lru.New(maxBadPeers)
	if err != nil {
		panic(err)
	}
	return &Peers{
		badPeers:  badPeers,
		penalties: penalties,
		host:      host,
	}
}

func (p *Peers) IsBadPeer(pid peer.ID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.badPeers.Contains(pid)
}

func (p *Peers) Penalize(pid peer.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	penaltyInterface, has := p.penalties.Get(pid)
	if !has {
		p.penalties.Add(pid, 1)
		return
	}
	penalties := penaltyInterface.(int) + 1

	p.penalties.Add(pid, penalties)
	// Drop peer and delete the map element.
	if penalties > MaxBadResponses {
		p.BanBadPeer(pid)
		p.penalties.Remove(pid)
	}
}

func (p *Peers) BanBadPeer(pid peer.ID) {
	p.DisconnectPeer(pid)
	p.badPeers.Add(pid, []byte{0})
	log.Debug("[Sentinel Peers] bad peers has been banned", "peer-id", pid)
}

func (p *Peers) DisconnectPeer(pid peer.ID) {
	log.Trace("[Sentinel Peers] disconnecting from peer", "peer-id", pid)
	p.host.Peerstore().RemovePeer(pid)
	p.host.Network().ClosePeer(pid)
}
