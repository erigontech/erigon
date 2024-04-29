package state

import libcommon "github.com/ledgerwatch/erigon-lib/common"

// BeaconStateReader is an interface for reading the beacon state.
//
//go:generate mockgen -typed=true -destination=./mock_services/beacon_state_reader_mock.go -package=mock_services . BeaconStateReader
type BeaconStateReader interface {
	ValidatorPublicKey(index int) (libcommon.Bytes48, error)
	GetDomain(domainType [4]byte, epoch uint64) ([]byte, error)
	CommitteeCount(epoch uint64) uint64
}
