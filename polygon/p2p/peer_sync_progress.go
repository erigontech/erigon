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
