package utils

import "time"

// compute current slot.
func GetCurrentSlot(genesisTime uint64, secondsPerSlot uint64) uint64 {
	now := uint64(time.Now().Unix())
	if now < genesisTime {
		return 0
	}

	return (now - genesisTime) / secondsPerSlot
}

// compute current epoch.
func GetCurrentEpoch(genesisTime uint64, secondsPerSlot uint64, slotsPerEpoch uint64) uint64 {
	now := uint64(time.Now().Unix())
	if now < genesisTime {
		return 0
	}

	return GetCurrentSlot(genesisTime, secondsPerSlot) / slotsPerEpoch
}
