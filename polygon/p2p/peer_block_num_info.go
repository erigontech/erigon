package p2p

type PeerBlockNumInfo struct {
	Id       PeerId
	BlockNum uint64
}

type PeerBlockNumInfos []*PeerBlockNumInfo

func (infos PeerBlockNumInfos) Len() int {
	return len(infos)
}

func (infos PeerBlockNumInfos) Less(i int, j int) bool {
	return infos[i].BlockNum < infos[j].BlockNum
}

func (infos PeerBlockNumInfos) Swap(i int, j int) {
	infos[i], infos[j] = infos[j], infos[i]
}
