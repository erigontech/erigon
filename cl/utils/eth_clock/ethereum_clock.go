// Copyright 2024 The Erigon Authors
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

package eth_clock

import (
	"encoding/binary"
	"sort"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
)

var maximumClockDisparity = 500 * time.Millisecond

//go:generate mockgen -typed=true -source=./ethereum_clock.go -destination=./ethereum_clock_mock.go -package=eth_clock . EthereumClock
type EthereumClock interface {
	GetSlotTime(slot uint64) time.Time
	GetCurrentSlot() uint64
	GetEpochAtSlot(slot uint64) uint64
	IsSlotCurrentSlotWithMaximumClockDisparity(slot uint64) bool
	GetSlotByTime(time time.Time) uint64
	GetCurrentEpoch() uint64
	CurrentForkDigest() (common.Bytes4, error)                             // ComputeForkDigest
	NextForkDigest() (common.Bytes4, error)                                // ComputeForkDigest
	ForkId() ([]byte, error)                                               // ComputeForkId
	LastFork() (common.Bytes4, error)                                      // GetLastFork
	StateVersionByForkDigest(common.Bytes4) (clparams.StateVersion, error) // ForkDigestVersion
	StateVersionByEpoch(uint64) clparams.StateVersion
	ComputeForkDigestForVersion(currentVersion common.Bytes4) (digest common.Bytes4, err error)

	GenesisValidatorsRoot() common.Hash
	GenesisTime() uint64
}

type forkNode struct {
	epoch        uint64
	stateVersion clparams.StateVersion
	version      [4]byte
}

func forkList(schedule map[common.Bytes4]clparams.VersionScheduleEntry) (f []forkNode) {
	for version, entry := range schedule {
		f = append(f, forkNode{epoch: entry.Epoch, version: version, stateVersion: entry.StateVersion})
	}
	sort.Slice(f, func(i, j int) bool {
		if f[i].epoch == f[j].epoch {
			return f[i].stateVersion < f[j].stateVersion
		}
		return f[i].epoch < f[j].epoch
	})
	return
}

type ethereumClockImpl struct {
	genesisTime           uint64
	genesisValidatorsRoot common.Hash
	beaconCfg             *clparams.BeaconChainConfig
}

func NewEthereumClock(genesisTime uint64, genesisValidatorsRoot common.Hash, beaconCfg *clparams.BeaconChainConfig) EthereumClock {
	return &ethereumClockImpl{
		genesisTime:           genesisTime,
		beaconCfg:             beaconCfg,
		genesisValidatorsRoot: genesisValidatorsRoot,
	}
}

func (t *ethereumClockImpl) GetSlotTime(slot uint64) time.Time {
	slotTime := t.genesisTime + t.beaconCfg.SecondsPerSlot*slot
	return time.Unix(int64(slotTime), 0)
}

func (t *ethereumClockImpl) GetCurrentSlot() uint64 {
	now := uint64(time.Now().Unix())
	if now < t.genesisTime {
		return 0
	}

	return (now - t.genesisTime) / t.beaconCfg.SecondsPerSlot
}

func (t *ethereumClockImpl) GetEpochAtSlot(slot uint64) uint64 {
	return slot / t.beaconCfg.SlotsPerEpoch
}

func (t *ethereumClockImpl) IsSlotCurrentSlotWithMaximumClockDisparity(slot uint64) bool {
	slotTime := t.GetSlotTime(slot)
	currSlot := t.GetCurrentSlot()
	minSlot := t.GetSlotByTime(slotTime.Add(-maximumClockDisparity))
	maxSlot := t.GetSlotByTime(slotTime.Add(maximumClockDisparity))
	return minSlot == currSlot || maxSlot == currSlot
}

func (t *ethereumClockImpl) GetSlotByTime(time time.Time) uint64 {
	return (uint64(time.Unix()) - t.genesisTime) / t.beaconCfg.SecondsPerSlot
}

func (t *ethereumClockImpl) GetCurrentEpoch() uint64 {
	now := uint64(time.Now().Unix())
	if now < t.genesisTime {
		return 0
	}

	return t.GetCurrentSlot() / t.beaconCfg.SlotsPerEpoch
}

func (t *ethereumClockImpl) CurrentForkDigest() (common.Bytes4, error) {
	currentEpoch := t.GetCurrentEpoch()
	// Retrieve current fork version.
	currentForkVersion := utils.Uint32ToBytes4(uint32(t.beaconCfg.GenesisForkVersion))
	for _, fork := range forkList(t.beaconCfg.ForkVersionSchedule) {
		if currentEpoch >= fork.epoch {
			currentForkVersion = fork.version
			continue
		}
		break
	}
	return t.ComputeForkDigestForVersion(currentForkVersion)
}

func (t *ethereumClockImpl) NextForkDigest() (common.Bytes4, error) {
	currentEpoch := t.GetCurrentEpoch()
	// Retrieve next fork version.
	nextForkIndex := 0
	forkList := forkList(t.beaconCfg.ForkVersionSchedule)
	for _, fork := range forkList {
		if currentEpoch >= fork.epoch {
			nextForkIndex++
			continue
		}
		break
	}
	if nextForkIndex-1 == len(forkList)-1 {
		return [4]byte{}, nil
	}
	return t.ComputeForkDigestForVersion(forkList[nextForkIndex].version)
}

func (t *ethereumClockImpl) ForkId() ([]byte, error) {
	digest, err := t.CurrentForkDigest()
	if err != nil {
		return nil, err
	}

	currentEpoch := t.GetCurrentEpoch()

	if time.Now().Unix() < int64(t.genesisTime) {
		currentEpoch = 0
	}

	var nextForkVersion [4]byte
	nextForkEpoch := uint64(0)
	for _, fork := range forkList(t.beaconCfg.ForkVersionSchedule) {
		if currentEpoch < fork.epoch {
			nextForkVersion = fork.version
			nextForkEpoch = fork.epoch
			break
		}
		nextForkVersion = fork.version
	}

	enrForkId := make([]byte, 16)
	copy(enrForkId, digest[:])
	copy(enrForkId[4:], nextForkVersion[:])
	binary.BigEndian.PutUint64(enrForkId[8:], nextForkEpoch)

	return enrForkId, nil
}

func (t *ethereumClockImpl) LastFork() (common.Bytes4, error) {
	currentEpoch := t.GetCurrentEpoch()
	// Retrieve current fork version.
	currentFork := utils.Uint32ToBytes4(uint32(t.beaconCfg.GenesisForkVersion))
	for _, fork := range forkList(t.beaconCfg.ForkVersionSchedule) {
		if currentEpoch >= fork.epoch {
			currentFork = fork.version
			continue
		}
		break
	}
	return currentFork, nil
}

func (t *ethereumClockImpl) StateVersionByEpoch(epoch uint64) clparams.StateVersion {
	return t.beaconCfg.GetCurrentStateVersion(epoch)
}

func (t *ethereumClockImpl) StateVersionByForkDigest(digest common.Bytes4) (clparams.StateVersion, error) {
	var (
		phase0ForkDigest, altairForkDigest, bellatrixForkDigest, capellaForkDigest, denebForkDigest common.Bytes4
		err                                                                                         error
	)
	phase0ForkDigest, err = t.ComputeForkDigestForVersion(utils.Uint32ToBytes4(uint32(t.beaconCfg.GenesisForkVersion)))
	if err != nil {
		return 0, err
	}

	altairForkDigest, err = t.ComputeForkDigestForVersion(utils.Uint32ToBytes4(uint32(t.beaconCfg.AltairForkVersion)))
	if err != nil {
		return 0, err
	}

	bellatrixForkDigest, err = t.ComputeForkDigestForVersion(utils.Uint32ToBytes4(uint32(t.beaconCfg.BellatrixForkVersion)))
	if err != nil {
		return 0, err
	}

	capellaForkDigest, err = t.ComputeForkDigestForVersion(utils.Uint32ToBytes4(uint32(t.beaconCfg.CapellaForkVersion)))
	if err != nil {
		return 0, err
	}

	denebForkDigest, err = t.ComputeForkDigestForVersion(utils.Uint32ToBytes4(uint32(t.beaconCfg.DenebForkVersion)))
	if err != nil {
		return 0, err
	}
	switch digest {
	case phase0ForkDigest:
		return clparams.Phase0Version, nil
	case altairForkDigest:
		return clparams.AltairVersion, nil
	case bellatrixForkDigest:
		return clparams.BellatrixVersion, nil
	case capellaForkDigest:
		return clparams.CapellaVersion, nil
	case denebForkDigest:
		return clparams.DenebVersion, nil
	}
	return 0, nil
}

func (t *ethereumClockImpl) ComputeForkDigestForVersion(currentVersion common.Bytes4) (digest common.Bytes4, err error) {
	var currentVersion32 common.Hash
	copy(currentVersion32[:], currentVersion[:])
	dataRoot := utils.Sha256(currentVersion32[:], t.genesisValidatorsRoot[:])
	// copy first four bytes to output
	copy(digest[:], dataRoot[:4])
	return
}

func (t *ethereumClockImpl) GenesisValidatorsRoot() common.Hash {
	return t.genesisValidatorsRoot
}

func (t *ethereumClockImpl) GenesisTime() uint64 {
	return t.genesisTime
}
