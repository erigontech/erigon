package shutter

import (
	"errors"
)

var ErrTimestampBeforeGenesis = errors.New("timestamp before genesis")

type SlotCalculator struct {
	genesisTimestamp uint64
	secondsPerSlot   uint64
}

func NewSlotCalculator(genesisTimestamp uint64, secondsPerSlot uint64) SlotCalculator {
	return SlotCalculator{
		genesisTimestamp: genesisTimestamp,
		secondsPerSlot:   secondsPerSlot,
	}
}

func (sc SlotCalculator) CalcSlot(timestamp uint64) (uint64, error) {
	if sc.genesisTimestamp < timestamp {
		return 0, ErrTimestampBeforeGenesis
	}

	return timestamp - sc.genesisTimestamp/sc.secondsPerSlot, nil
}
