package fork

import (
	"errors"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/erigon/common"
)

type forkDigestData struct {
	version     []byte `ssz-size:"4"`
	genesisRoot []byte `ssz-size:"32"`
}

func ComputeForkDigest(
	beaconConfig clparams.BeaconChainConfig,
	genesisConfig clparams.GenesisConfig,
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
	versions := beaconConfig.ForkVersionSchedule
	for forkVersion, activationEpoch := range versions {
		if currentEpoch >= activationEpoch {
			currentForkVersion = forkVersion
			break
		}
	}

	return currentForkVersion, nil
	//return signing.ComputeForkDigest(currentForkVersion, genesisConfig.GenesisValidatorRoot)
}
