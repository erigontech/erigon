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

package clparams

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T, n NetworkType) {
	genesis, network, beacon := GetConfigsByNetwork(n)

	require.Equal(t, *genesis, GenesisConfigs[n])
	require.Equal(t, *network, NetworkConfigs[n])
	require.Equal(t, *beacon, BeaconConfigs[n])
}

func TestGetConfigsByNetwork(t *testing.T) {
	testConfig(t, MainnetNetwork)
	testConfig(t, SepoliaNetwork)
	testConfig(t, GoerliNetwork)
}
