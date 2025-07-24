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
	"math"
	"slices"
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
	CurrentForkDigest() (common.Bytes4, error) // ComputeForkDigest
	NextForkDigest() (common.Bytes4, error)    // ComputeForkDigest
	NextForkEpochIncludeBPO() uint64
	ForkId() ([]byte, error)                                               // ComputeForkId
	LastFork() (common.Bytes4, error)                                      // GetLastFork
	StateVersionByForkDigest(common.Bytes4) (clparams.StateVersion, error) // ForkDigestVersion
	StateVersionByEpoch(uint64) clparams.StateVersion
	//ComputeForkDigestForVersion(currentVersion common.Bytes4) (digest common.Bytes4, err error)
	ComputeForkDigest(epoch uint64) (digest common.Bytes4, err error) // new in fulu

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
	forkDigestToVersion   map[common.Bytes4]clparams.StateVersion
}

func NewEthereumClock(genesisTime uint64, genesisValidatorsRoot common.Hash, beaconCfg *clparams.BeaconChainConfig) EthereumClock {
	impl := &ethereumClockImpl{
		genesisTime:           genesisTime,
		beaconCfg:             beaconCfg,
		genesisValidatorsRoot: genesisValidatorsRoot,
		forkDigestToVersion:   make(map[common.Bytes4]clparams.StateVersion),
	}

	for _, fork := range forkList(beaconCfg.ForkVersionSchedule) {
		digest, err := impl.computeForkDigestForVersion(fork.version)
		if err != nil {
			panic(err)
		}
		impl.forkDigestToVersion[digest] = fork.stateVersion
	}
	return impl
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
	return t.ComputeForkDigest(currentEpoch)
}

func (t *ethereumClockImpl) NextForkDigest() (common.Bytes4, error) {
	nextForkEpoch := t.NextForkEpochIncludeBPO()
	if nextForkEpoch == t.beaconCfg.FarFutureEpoch {
		return [4]byte{}, nil
	}
	return t.ComputeForkDigest(nextForkEpoch)
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
	for _, fork := range forkList(t.beaconCfg.ForkVersionSchedule) {
		if currentEpoch < fork.epoch {
			nextForkVersion = fork.version
			break
		}
		nextForkVersion = fork.version
	}

	enrForkId := make([]byte, 16)
	copy(enrForkId, digest[:])                                             // current fork digest
	copy(enrForkId[4:], nextForkVersion[:])                                // next fork version
	binary.BigEndian.PutUint64(enrForkId[8:], t.NextForkEpochIncludeBPO()) // next fork epoch
	return enrForkId, nil
}

func (t *ethereumClockImpl) NextForkEpochIncludeBPO() uint64 {
	// collect all fork epochs
	forkEpochs := make([]uint64, 0)
	for _, fork := range forkList(t.beaconCfg.ForkVersionSchedule) {
		forkEpochs = append(forkEpochs, fork.epoch)
	}
	// collect all BPO epochs
	for _, blobSchedule := range t.beaconCfg.BlobSchedule {
		forkEpochs = append(forkEpochs, blobSchedule.Epoch)
	}
	slices.Sort(forkEpochs)
	// find the next fork epoch
	currentEpoch := t.GetCurrentEpoch()
	nextForkEpoch := t.beaconCfg.FarFutureEpoch
	for _, forkEpoch := range forkEpochs {
		if forkEpoch > currentEpoch {
			nextForkEpoch = forkEpoch
			break
		}
	}
	if nextForkEpoch == math.MaxUint64 {
		nextForkEpoch = t.beaconCfg.FarFutureEpoch
	}
	return nextForkEpoch
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
	stateVersion, ok := t.forkDigestToVersion[digest]
	if ok {
		return stateVersion, nil
	}

	return clparams.FuluVersion, nil
}

func (t *ethereumClockImpl) computeForkDigestForVersion(currentVersion common.Bytes4) (digest common.Bytes4, err error) {
	dataRoot := computeForkDataRoot(currentVersion, t.genesisValidatorsRoot)
	// copy first four bytes to output
	copy(digest[:], dataRoot[:4])
	return
}

func (t *ethereumClockImpl) ComputeForkDigest(epoch uint64) (digest common.Bytes4, err error) {
	// Get fork version for epoch
	stateVersion := t.beaconCfg.GetCurrentStateVersion(epoch)
	version := t.beaconCfg.GetForkVersionByVersion(stateVersion)
	forkVersion := utils.Uint32ToBytes4(version)

	// Compute base digest from fork version and genesis validators root
	baseDigest := computeForkDataRoot(forkVersion, t.genesisValidatorsRoot)

	if stateVersion < clparams.FuluVersion {
		digest = common.Bytes4{}
		copy(digest[:], baseDigest[:4])
		return
	}

	// For Fulu and later, XOR base digest with hash of blob parameters
	blobParams := t.beaconCfg.GetBlobParameters(epoch)

	// Hash blob parameters (epoch and max_blobs_per_block)
	blobParamsBytes := make([]byte, 16)
	binary.LittleEndian.PutUint64(blobParamsBytes[:8], blobParams.Epoch)
	binary.LittleEndian.PutUint64(blobParamsBytes[8:], blobParams.MaxBlobsPerBlock)
	blobParamsHash := utils.Sha256(blobParamsBytes)

	// XOR first 4 bytes of base digest with first 4 bytes of blob params hash
	digest = common.Bytes4{}
	for i := 0; i < 4; i++ {
		digest[i] = baseDigest[i] ^ blobParamsHash[i]
	}

	return digest, nil
}

func (t *ethereumClockImpl) GenesisValidatorsRoot() common.Hash {
	return t.genesisValidatorsRoot
}

func (t *ethereumClockImpl) GenesisTime() uint64 {
	return t.genesisTime
}

func computeForkDataRoot(version [4]byte, genesisValidatorsRoot common.Hash) common.Hash {
	var currentVersion32 common.Hash
	copy(currentVersion32[:], version[:])
	return utils.Sha256(currentVersion32[:], genesisValidatorsRoot[:])
}
