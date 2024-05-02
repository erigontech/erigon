package synced_data

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

//go:generate mockgen -typed=true -destination=./mock_services/synced_data_mock.go -package=mock_services . SyncedData
type SyncedData interface {
	OnHeadState(newState *state.CachingBeaconState) (err error)
	HeadState() *state.CachingBeaconState
	HeadStateReader() abstract.BeaconStateReader
	Syncing() bool
	HeadSlot() uint64
}
