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
	"errors"
	"math"
	"sort"
	"time"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
)

func ComputeForkDigest(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) ([4]byte, error) {
	if genesisConfig.GenesisTime == 0 {
		return [4]byte{}, errors.New("genesis time is not set")
	}
	if genesisConfig.GenesisValidatorRoot == (common.Hash{}) {
		return [4]byte{}, errors.New("genesis validators root is not set")
	}

	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)
	// Retrieve current fork version.
	currentForkVersion := utils.BytesToBytes4(beaconConfig.GenesisForkVersion)
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
	data := cltypes.ForkData{
		CurrentVersion:        currentVersion,
		GenesisValidatorsRoot: genesisValidatorsRoot,
	}
	var dataRoot [32]byte
	dataRoot, err = data.HashTreeRoot()
	if err != nil {
		return
	}
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

	enrForkID := cltypes.ENRForkID{
		CurrentForkDigest: digest,
		NextForkVersion:   nextForkVersion,
		NextForkEpoch:     nextForkEpoch,
	}
	return enrForkID.MarshalSSZ()
}

func GetLastFork(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) [4]byte {
	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)
	// Retrieve current fork version.
	currentFork := utils.BytesToBytes4(beaconConfig.GenesisForkVersion)
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
	forkDataRoot, err := (&cltypes.ForkData{
		CurrentVersion:        currentVersion,
		GenesisValidatorsRoot: genesisValidatorsRoot,
	}).HashTreeRoot()
	if err != nil {
		return nil, err
	}
	return append(domainType, forkDataRoot[:28]...), nil
}

func ComputeSigningRoot(
	obj cltypes.ObjectSSZ,
	domain []byte,
) ([32]byte, error) {
	objRoot, err := obj.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	return (&cltypes.SigningData{
		Root:   objRoot,
		Domain: domain,
	}).HashTreeRoot()
}
