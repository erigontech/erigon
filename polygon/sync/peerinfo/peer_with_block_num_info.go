package peerinfo

import "math/big"

type PeerWithBlockNumInfo struct {
	ID       string
	BlockNum *big.Int
}

type PeersWithBlockNumInfo []*PeerWithBlockNumInfo

func (peers PeersWithBlockNumInfo) Len() int {
	return len(peers)
}

func (peers PeersWithBlockNumInfo) Less(i int, j int) bool {
	return peers[i].BlockNum.Cmp(peers[j].BlockNum) < 1
}

func (peers PeersWithBlockNumInfo) Swap(i int, j int) {
	peers[i], peers[j] = peers[j], peers[i]
}
