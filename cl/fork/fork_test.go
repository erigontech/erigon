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
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

// Testing Fork digest
func TestMainnetForkDigest(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	_, err = ComputeForkId(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, [4]byte{0xbb, 0xa4, 0xda, 0x96}, digest)
}

func TestMainnetForkDigestWithNoGenesisTime(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	genesisCfg.GenesisTime = 0
	_, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.ErrorIs(t, err, NO_GENESIS_TIME_ERR)
}

func TestMainnerForkDigestWithNoValidatorRootHash(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	genesisCfg.GenesisValidatorRoot = common.Hash{}
	_, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.ErrorIs(t, err, NO_VALIDATOR_ROOT_HASH)
}

func TestGoerliForkDigest(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.GoerliNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.GoerliNetwork]
	digest, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	_, err = ComputeForkId(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, [4]uint8{0x62, 0x89, 0x41, 0xef}, digest)
}

func TestSepoliaForkDigest(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.SepoliaNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.SepoliaNetwork]
	digest, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	_, err = ComputeForkId(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, [4]uint8{0x47, 0xeb, 0x72, 0xb3}, digest)
}

func TestGnosisForkDigest(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.GnosisNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.GnosisNetwork]
	digest, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	_, err = ComputeForkId(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, [4]uint8{0x82, 0x4b, 0xe4, 0x31}, digest)
}

// ForkDigestVersion
func TestMainnetForkDigestPhase0Version(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigestForVersion(utils.Uint32ToBytes4(beaconCfg.GenesisForkVersion), genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	version, err := ForkDigestVersion(digest, &beaconCfg, genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	require.Equal(t, clparams.Phase0Version, version)
}

func TestMainnetForkDigestAltairVersion(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigestForVersion(utils.Uint32ToBytes4(beaconCfg.AltairForkVersion), genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	version, err := ForkDigestVersion(digest, &beaconCfg, genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	require.Equal(t, clparams.AltairVersion, version)
}

func TestMainnetForkDigestBellatrixVersion(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigestForVersion(utils.Uint32ToBytes4(beaconCfg.BellatrixForkVersion), genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	version, err := ForkDigestVersion(digest, &beaconCfg, genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	require.Equal(t, clparams.BellatrixVersion, version)
}

func TestMainnetForkDigestCapellaVersion(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigestForVersion(utils.Uint32ToBytes4(beaconCfg.CapellaForkVersion), genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	version, err := ForkDigestVersion(digest, &beaconCfg, genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	require.Equal(t, clparams.CapellaVersion, version)
}

func TestMainnetForkDigestDenebVersion(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigestForVersion(utils.Uint32ToBytes4(beaconCfg.DenebForkVersion), genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	version, err := ForkDigestVersion(digest, &beaconCfg, genesisCfg.GenesisValidatorRoot)
	require.NoError(t, err)
	require.Equal(t, clparams.DenebVersion, version)
}

// ComputeForkNextDigest
func TestMainnetComputeForkNextDigest(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	beaconCfg.ForkVersionSchedule = make(map[[4]byte]uint64)
	beaconCfg.ForkVersionSchedule[utils.Uint32ToBytes4(uint32(clparams.Phase0Version))] = 0
	beaconCfg.ForkVersionSchedule[utils.Uint32ToBytes4(uint32(clparams.BellatrixVersion))] = 210010230210301201
	digest, err := ComputeNextForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, [4]uint8{0xe, 0x6, 0x3, 0x18}, digest)
}

func TestMainnetComputeDomain(t *testing.T) {
	domainType := [4]uint8{0x1, 0x0, 0x0, 0x0}
	currentVersion := [4]uint8{0x3, 0x0, 0x0, 0x0}
	genesesis := [32]uint8{0x4b, 0x36, 0x3d, 0xb9, 0x4e, 0x28, 0x61, 0x20, 0xd7, 0x6e, 0xb9, 0x5, 0x34, 0xf, 0xdd, 0x4e, 0x54, 0xbf, 0xe9, 0xf0, 0x6b, 0xf3, 0x3f, 0xf6,
		0xcf, 0x5a, 0xd2, 0x7f, 0x51, 0x1b, 0xfe, 0x95}

	expectedResult := []byte{0x1, 0x0, 0x0, 0x0, 0xbb, 0xa4, 0xda, 0x96, 0x35, 0x4c, 0x9f, 0x25, 0x47, 0x6c, 0xf1, 0xbc, 0x69, 0xbf, 0x58, 0x3a, 0x7f, 0x9e, 0xa, 0xf0, 0x49, 0x30, 0x5b, 0x62, 0xde, 0x67, 0x66, 0x40}

	result, err := ComputeDomain(domainType[:], currentVersion, genesesis)
	require.NoError(t, err)
	require.Equal(t, expectedResult, result)
}
