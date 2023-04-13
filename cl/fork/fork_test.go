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

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestMainnetFork(t *testing.T) {
	beaconCfg := clparams.BeaconConfigs[clparams.MainnetNetwork]
	genesisCfg := clparams.GenesisConfigs[clparams.MainnetNetwork]
	digest, err := ComputeForkDigest(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	_, err = ComputeForkId(&beaconCfg, &genesisCfg)
	require.NoError(t, err)
	require.Equal(t, [4]byte{0xbb, 0xa4, 0xda, 0x96}, digest)
}
