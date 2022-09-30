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
	maxBadPeers     = 10_000
	DefaultMaxPeers = 33
	MaxBadResponses = 10
)

type Peers struct {
	badPeers     *lru.Cache
	badResponses map[peer.ID]int
	host         host.Host
	mu           sync.Mutex
}

func New(host host.Host) *Peers {
	badPeers, err := lru.New(maxBadPeers)
	if err != nil {
		panic(err)
	}
	return &Peers{
		badPeers:     badPeers,
		badResponses: make(map[peer.ID]int),
		host:         host,
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
	if _, has := p.badResponses[pid]; !has {
		p.badResponses[pid] = 1
	}

	p.badResponses[pid]++
	// Drop peer and delete the map element.
	if p.badResponses[pid] > MaxBadResponses {
		p.banBadPeer(pid)
		delete(p.badResponses, pid)
	}
}

func (p *Peers) banBadPeer(pid peer.ID) {
	p.DisconnectPeer(pid)
	p.badPeers.Add(pid, []byte{0})
	log.Warn("[Peers] bad peers has been banned", "peer-id", pid)
}

func (p *Peers) DisconnectPeer(pid peer.ID) {
	log.Trace("[Peers] disconnecting from peer", "peer-id", pid)
	p.host.Peerstore().RemovePeer(pid)
}
