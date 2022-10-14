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

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/erigon/common"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
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

	return computeForkDigest(currentForkVersion, genesisConfig.GenesisValidatorRoot)
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

func computeForkDigest(currentVersion [4]byte, genesisValidatorsRoot [32]byte) (digest [4]byte, err error) {
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

func getLastForkEpoch(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) uint64 {
	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)
	// Retrieve current fork version.
	currentForkEpoch := beaconConfig.GenesisEpoch
	for _, fork := range forkList(beaconConfig.ForkVersionSchedule) {
		if currentEpoch >= fork.epoch {
			currentForkEpoch = fork.epoch
			continue
		}
		break
	}
	return currentForkEpoch
}

// The one suggested by the spec is too over-engineered.
func MsgID(pmsg *pubsubpb.Message) string {
	hash := utils.Keccak256(pmsg.Data)
	return string(hash[:])
}
