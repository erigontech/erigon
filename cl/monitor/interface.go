package monitor

import (
	"github.com/erigontech/erigon/cl/cltypes"
)

//go:generate mockgen -typed=true -destination=mock_services/validator_monitor_mock.go -package=mock_services . ValidatorMonitor
type ValidatorMonitor interface {
	ObserveValidator(vid uint64)
	RemoveValidator(vid uint64)
	OnNewBlock(block *cltypes.BeaconBlock) error
}

type dummyValdatorMonitor struct{}

func (d *dummyValdatorMonitor) ObserveValidator(vid uint64) {}

func (d *dummyValdatorMonitor) RemoveValidator(vid uint64) {}

func (d *dummyValdatorMonitor) OnNewBlock(block *cltypes.BeaconBlock) error {
	return nil
}
