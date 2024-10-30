package monitor

import (
	"github.com/erigontech/erigon/v3/cl/cltypes"
	"github.com/erigontech/erigon/v3/cl/phase1/core/state"
)

//go:generate mockgen -typed=true -destination=mock_services/validator_monitor_mock.go -package=mock_services . ValidatorMonitor
type ValidatorMonitor interface {
	ObserveValidator(vid uint64)
	RemoveValidator(vid uint64)
	OnNewBlock(state *state.CachingBeaconState, block *cltypes.BeaconBlock) error
}

type dummyValdatorMonitor struct{}

func (d *dummyValdatorMonitor) ObserveValidator(vid uint64) {}

func (d *dummyValdatorMonitor) RemoveValidator(vid uint64) {}

func (d *dummyValdatorMonitor) OnNewBlock(_ *state.CachingBeaconState, _ *cltypes.BeaconBlock) error {
	return nil
}
