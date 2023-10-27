/*
   Copyright 2023 The Erigon contributors

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

package chain

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon-lib/common"
)

func TestBorKeyValueConfigHelper(t *testing.T) {
	backupMultiplier := map[string]uint64{
		"0":        2,
		"25275000": 5,
		"29638656": 2,
	}
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 0), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 25275000-1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 25275000), uint64(5))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 25275000+1), uint64(5))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 29638656-1), uint64(5))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 29638656), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 29638656+1), uint64(2))

	config := map[string]uint64{
		"0":         1,
		"90000000":  2,
		"100000000": 3,
	}
	assert.Equal(t, borKeyValueConfigHelper(config, 0), uint64(1))
	assert.Equal(t, borKeyValueConfigHelper(config, 1), uint64(1))
	assert.Equal(t, borKeyValueConfigHelper(config, 90000000-1), uint64(1))
	assert.Equal(t, borKeyValueConfigHelper(config, 90000000), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(config, 90000000+1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(config, 100000000-1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(config, 100000000), uint64(3))
	assert.Equal(t, borKeyValueConfigHelper(config, 100000000+1), uint64(3))

	address1 := common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38")
	address2 := common.HexToAddress("0x617b94CCCC2511808A3C9478ebb96f455CF167aA")

	burntContract := map[string]common.Address{
		"22640000": address1,
		"41824608": address2,
	}
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 22640000), address1)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 22640000+1), address1)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 41824608-1), address1)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 41824608), address2)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 41824608+1), address2)
}
