package testhelpers

import "github.com/erigontech/erigon/txnprovider/shutter"

//go:generate mockgen -typed=true -destination=./slot_calculator_mock.go -package=testhelpers . SlotCalculator
type SlotCalculator interface {
	shutter.SlotCalculator
}

//go:generate mockgen -typed=true -destination=./eon_tracker_mock.go -package=testhelpers . EonTracker
type EonTracker interface {
	shutter.EonTracker
}
