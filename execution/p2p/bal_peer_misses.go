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

package p2p

import (
	"sync"
	"time"
)

// balPeerMisses remembers, per peer, the highest block number the peer
// answered "don't have" for (explicit 0x80 or a wrong-empty violation). BAL
// retention keeps a recent window, so a miss at N implies everything below N
// is missing too; the mark expires so retention growth is re-probed.
type balPeerMisses struct {
	mu sync.Mutex
	m  map[PeerId]balMissMark
}

type balMissMark struct {
	maxMissing uint64
	at         time.Time
}

const balMissTTL = 2 * time.Minute

func (b *balPeerMisses) mark(peerId PeerId, blockNum uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.m == nil {
		b.m = map[PeerId]balMissMark{}
	}
	mark := b.m[peerId]
	if blockNum > mark.maxMissing || time.Since(mark.at) > balMissTTL {
		mark.maxMissing = max(blockNum, mark.maxMissing)
	}
	mark.at = time.Now()
	b.m[peerId] = mark
}

func (b *balPeerMisses) mayHave(peerId PeerId, blockNum uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	mark, ok := b.m[peerId]
	if !ok || time.Since(mark.at) > balMissTTL {
		return true
	}
	return blockNum > mark.maxMissing
}
