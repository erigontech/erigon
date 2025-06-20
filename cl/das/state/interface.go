package peerdasstate

type PeerDasStateReader interface {
	GetEarliestAvailableSlot() uint64
	GetRealCgc() uint64
	GetAdvertisedCgc() uint64
}
