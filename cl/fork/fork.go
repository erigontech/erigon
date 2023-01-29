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

package fork

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func ForkDigestVersion(digest [4]byte, b *clparams.BeaconChainConfig, genesisValidatorRoot libcommon.Hash) (clparams.StateVersion, error) {
	var (
		phase0ForkDigest, altairForkDigest, bellatrixForkDigest, capellaForkDigest [4]byte
		err                                                                        error
	)
	phase0ForkDigest, err = ComputeForkDigestForVersion(
		utils.Uint32ToBytes4(b.GenesisForkVersion),
		genesisValidatorRoot,
	)
	if err != nil {
		return 0, err
	}

	altairForkDigest, err = ComputeForkDigestForVersion(
		utils.Uint32ToBytes4(b.AltairForkVersion),
		genesisValidatorRoot,
	)
	if err != nil {
		return 0, err
	}

	bellatrixForkDigest, err = ComputeForkDigestForVersion(
		utils.Uint32ToBytes4(b.BellatrixForkVersion),
		genesisValidatorRoot,
	)
	if err != nil {
		return 0, err
	}

	capellaForkDigest, err = ComputeForkDigestForVersion(
		utils.Uint32ToBytes4(b.CapellaForkVersion),
		genesisValidatorRoot,
	)
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
	}
	return 0, fmt.Errorf("invalid state version")
}

func ComputeForkDigest(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) ([4]byte, error) {
	if genesisConfig.GenesisTime == 0 {
		return [4]byte{}, errors.New("genesis time is not set")
	}
	if genesisConfig.GenesisValidatorRoot == (libcommon.Hash{}) {
		return [4]byte{}, errors.New("genesis validators root is not set")
	}

	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)
	// Retrieve current fork version.
	currentForkVersion := utils.Uint32ToBytes4(beaconConfig.GenesisForkVersion)
	for _, fork := range forkList(beaconConfig.ForkVersionSchedule) {
		if currentEpoch >= fork.epoch {
			currentForkVersion = fork.version
			continue
		}
		break
	}

	return ComputeForkDigestForVersion(currentForkVersion, genesisConfig.GenesisValidatorRoot)
}

type fork struct {
	epoch   uint64
	version [4]byte
}

func forkList(schedule map[[4]byte]uint64) (f []fork) {
	for version, epoch := range schedule {
		f = append(f, fork{epoch: epoch, version: version})
	}
	sort.Slice(f, func(i, j int) bool {
		return f[i].epoch < f[j].epoch
	})
	return
}

func ComputeForkDigestForVersion(currentVersion [4]byte, genesisValidatorsRoot [32]byte) (digest [4]byte, err error) {
	var currentVersion32 libcommon.Hash
	copy(currentVersion32[:], currentVersion[:])
	dataRoot := utils.Keccak256(currentVersion32[:], genesisValidatorsRoot[:])
	// copy first four bytes to output
	copy(digest[:], dataRoot[:4])
	return
}

func ComputeForkId(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) ([]byte, error) {
	digest, err := ComputeForkDigest(beaconConfig, genesisConfig)
	if err != nil {
		return nil, err
	}

	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)

	if time.Now().Unix() < int64(genesisConfig.GenesisTime) {
		currentEpoch = 0
	}

	var nextForkVersion [4]byte
	nextForkEpoch := uint64(math.MaxUint64)
	for _, fork := range forkList(beaconConfig.ForkVersionSchedule) {
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

func GetLastFork(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) [4]byte {
	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)
	// Retrieve current fork version.
	currentFork := utils.Uint32ToBytes4(beaconConfig.GenesisForkVersion)
	for _, fork := range forkList(beaconConfig.ForkVersionSchedule) {
		if currentEpoch >= fork.epoch {
			currentFork = fork.version
			continue
		}
		break
	}
	return currentFork
}

func ComputeDomain(
	domainType []byte,
	currentVersion [4]byte,
	genesisValidatorsRoot [32]byte,
) ([]byte, error) {
	var currentVersion32 libcommon.Hash
	copy(currentVersion32[:], currentVersion[:])
	forkDataRoot := utils.Keccak256(currentVersion32[:], genesisValidatorsRoot[:])
	return append(domainType, forkDataRoot[:28]...), nil
}

func ComputeSigningRoot(
	obj ssz_utils.HashableSSZ,
	domain []byte,
) ([32]byte, error) {
	objRoot, err := obj.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return utils.Keccak256(objRoot[:], domain), nil
}
