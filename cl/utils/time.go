/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package utils

import "time"

// compute time of slot.
func GetSlotTime(genesisTime uint64, secondsPerSlot uint64, slot uint64) time.Time {
	slotTime := genesisTime + secondsPerSlot*slot
	return time.Unix(int64(slotTime), 0)
}

// compute current slot.
func GetCurrentSlot(genesisTime uint64, secondsPerSlot uint64) uint64 {
	now := uint64(time.Now().Unix())
	if now < genesisTime {
		return 0
	}

	return (now - genesisTime) / secondsPerSlot
}

// compute current slot.
func GetCurrentSlotOverTime(genesisTime uint64, secondsPerSlot uint64) uint64 {
	now := uint64(time.Now().Unix())
	if now < genesisTime {
		return 0
	}

	return (now - genesisTime) % secondsPerSlot
}

// compute current epoch.
func GetCurrentEpoch(genesisTime uint64, secondsPerSlot uint64, slotsPerEpoch uint64) uint64 {
	now := uint64(time.Now().Unix())
	if now < genesisTime {
		return 0
	}

	return GetCurrentSlot(genesisTime, secondsPerSlot) / slotsPerEpoch
}

// compute current slot.
func SlotToPeriod(slot uint64) uint64 {
	return slot / 8192
}
