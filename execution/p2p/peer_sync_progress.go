// Copyright 2024 The Erigon Authors
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

import "time"

const missingBlockNumExpiry = time.Hour

type peerSyncProgress struct {
	peerId               *PeerId
	minMissingBlockNum   uint64
	minMissingBlockNumTs time.Time
}

func (psp *peerSyncProgress) blockNumPresent(blockNum uint64) {
	if psp.minMissingBlockNum <= blockNum {
		psp.minMissingBlockNum = 0
		psp.minMissingBlockNumTs = time.Time{}
	}
}

func (psp *peerSyncProgress) blockNumMissing(blockNum uint64) {
	if psp.minMissingBlockNum >= blockNum || psp.minMissingBlockNumTsExpired() {
		psp.minMissingBlockNum = blockNum
		psp.minMissingBlockNumTs = time.Now()
	}
}

func (psp *peerSyncProgress) peerMayHaveBlockNum(blockNum uint64) bool {
	// Currently, we simplify the problem by assuming that if a peer does not have block X
	// then it does not have any blocks >= X for some time Y. In some time Y we can try again to
	// see if it has blocks >= X. This works ok for the purposes of the initial sync.
	// In the future we can explore more sophisticated heuristics and keep track of more parameters if needed.
	if psp.minMissingBlockNumTsExpired() || psp.minMissingBlockNum == 0 {
		return true
	}

	return blockNum < psp.minMissingBlockNum
}

func (psp *peerSyncProgress) minMissingBlockNumTsExpired() bool {
	return time.Now().After(psp.minMissingBlockNumTs.Add(missingBlockNumExpiry))
}
