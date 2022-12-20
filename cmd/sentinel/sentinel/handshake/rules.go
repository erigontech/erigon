package handshake

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type RuleFunc func(
	status *cltypes.Status,
	ourStatus *cltypes.Status,
	genesisConfig *clparams.GenesisConfig,
	beaconConfig *clparams.BeaconChainConfig,
) bool

// NoRule no rules in place.
func NoRule(status *cltypes.Status, ourStatus *cltypes.Status, genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig) bool {
	return true
}

// FullClientRule only checks against fork digest.
func FullClientRule(status *cltypes.Status, ourStatus *cltypes.Status, genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig) bool {
	forkDigest, err := fork.ComputeForkDigest(beaconConfig, genesisConfig)
	if err != nil {
		return false
	}

	accept := status.ForkDigest == forkDigest
	if ourStatus.HeadSlot != utils.GetCurrentSlot(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot) {
		accept = accept && ourStatus.HeadSlot < status.HeadSlot
	}
	return accept
}

// LightClientRule checks against fork digest and whether the peer head is on chain tip.
func LightClientRule(status *cltypes.Status, ourStatus *cltypes.Status, genesisConfig *clparams.GenesisConfig, beaconConfig *clparams.BeaconChainConfig) bool {
	forkDigest, err := fork.ComputeForkDigest(beaconConfig, genesisConfig)
	if err != nil {
		return false
	}
	return status.ForkDigest == forkDigest &&
		status.HeadSlot == utils.GetCurrentSlot(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot)
}
