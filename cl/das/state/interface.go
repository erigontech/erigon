package peerdasstate

import "github.com/erigontech/erigon/cl/cltypes"

//go:generate mockgen -typed=true -destination=mock_services/peer_das_state_reader_mock.go -package=mock_services . PeerDasStateReader
type PeerDasStateReader interface {
	GetEarliestAvailableSlot() uint64
	GetRealCgc() uint64
	GetAdvertisedCgc() uint64
	GetMyCustodyColumns() (map[cltypes.CustodyIndex]bool, error)
}
