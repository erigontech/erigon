package shutter

import (
	"errors"
	"time"
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

	return (timestamp - sc.genesisTimestamp) / sc.secondsPerSlot, nil
}

func (sc SlotCalculator) CalcSlotAge(slot uint64) time.Duration {
	slotStartTimestamp := sc.genesisTimestamp + slot*sc.secondsPerSlot
	return time.Since(time.Unix(int64(slotStartTimestamp), 0))
}

func (sc SlotCalculator) CalcCurrentSlot() uint64 {
	slot, err := sc.CalcSlot(uint64(time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	return slot
}
