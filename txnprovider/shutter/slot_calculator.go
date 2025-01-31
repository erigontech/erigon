// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package shutter

import (
	"errors"
	"time"
)

var ErrTimestampBeforeGenesis = errors.New("timestamp before genesis")

type SlotCalculator interface {
	CalcSlot(timestamp uint64) (uint64, error)
	CalcSlotAge(slot uint64) time.Duration
	CalcCurrentSlot() uint64
}

type BeaconChainSlotCalculator struct {
	genesisTimestamp uint64
	secondsPerSlot   uint64
}

func NewBeaconChainSlotCalculator(genesisTimestamp uint64, secondsPerSlot uint64) BeaconChainSlotCalculator {
	return BeaconChainSlotCalculator{
		genesisTimestamp: genesisTimestamp,
		secondsPerSlot:   secondsPerSlot,
	}
}

func (sc BeaconChainSlotCalculator) CalcSlot(timestamp uint64) (uint64, error) {
	if sc.genesisTimestamp < timestamp {
		return 0, ErrTimestampBeforeGenesis
	}

	return (timestamp - sc.genesisTimestamp) / sc.secondsPerSlot, nil
}

func (sc BeaconChainSlotCalculator) CalcSlotAge(slot uint64) time.Duration {
	slotStartTimestamp := sc.genesisTimestamp + slot*sc.secondsPerSlot
	return time.Since(time.Unix(int64(slotStartTimestamp), 0))
}

func (sc BeaconChainSlotCalculator) CalcCurrentSlot() uint64 {
	slot, err := sc.CalcSlot(uint64(time.Now().Unix()))
	if err != nil {
		panic(err)
	}

	return slot
}
