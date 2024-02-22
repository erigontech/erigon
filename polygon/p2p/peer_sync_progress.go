package p2p

type PeerSyncProgress struct {
	Id              PeerId
	MaxSeenBlockNum uint64
}

type PeersSyncProgress []*PeerSyncProgress

func (psp PeersSyncProgress) Len() int {
	return len(psp)
}

func (psp PeersSyncProgress) Less(i int, j int) bool {
	return psp[i].MaxSeenBlockNum < psp[j].MaxSeenBlockNum
}

func (psp PeersSyncProgress) Swap(i int, j int) {
	psp[i], psp[j] = psp[j], psp[i]
}
