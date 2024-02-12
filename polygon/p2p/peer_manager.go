package p2p

//go:generate mockgen -destination=./peer_manager_mock.go -package=p2p . PeerManager
type PeerManager interface {
	MaxPeers() int
	PeersSyncProgress() PeersSyncProgress
	Penalize(pid PeerId)
}

func NewPeerManager() PeerManager {
	return &peerManager{}
}

type peerManager struct{}

func (pm *peerManager) MaxPeers() int {
	//TODO implement me
	panic("implement me")
}

func (pm *peerManager) PeersSyncProgress() PeersSyncProgress {
	//TODO implement me
	panic("implement me")
}

func (pm *peerManager) Penalize(_ PeerId) {
	//TODO implement me
	panic("implement me")
}
