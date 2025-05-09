// Copyright 2023 The Erigon Authors
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

package chain

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
)

func TestConfigValueLookup(t *testing.T) {
	backupMultiplier := map[string]uint64{
		"0":        2,
		"25275000": 5,
		"29638656": 2,
	}
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 0))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 1))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 25275000-1))
	assert.Equal(t, uint64(5), ConfigValueLookup(backupMultiplier, 25275000))
	assert.Equal(t, uint64(5), ConfigValueLookup(backupMultiplier, 25275000+1))
	assert.Equal(t, uint64(5), ConfigValueLookup(backupMultiplier, 29638656-1))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 29638656))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 29638656+1))

	config := map[string]uint64{
		"0":         1,
		"90000000":  2,
		"100000000": 3,
	}
	assert.Equal(t, uint64(1), ConfigValueLookup(config, 0))
	assert.Equal(t, uint64(1), ConfigValueLookup(config, 1))
	assert.Equal(t, uint64(1), ConfigValueLookup(config, 90000000-1))
	assert.Equal(t, uint64(2), ConfigValueLookup(config, 90000000))
	assert.Equal(t, uint64(2), ConfigValueLookup(config, 90000000+1))
	assert.Equal(t, uint64(2), ConfigValueLookup(config, 100000000-1))
	assert.Equal(t, uint64(3), ConfigValueLookup(config, 100000000))
	assert.Equal(t, uint64(3), ConfigValueLookup(config, 100000000+1))

	address1 := common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38")
	address2 := common.HexToAddress("0x617b94CCCC2511808A3C9478ebb96f455CF167aA")

	burntContract := map[string]common.Address{
		"22640000": address1,
		"41874000": address2,
	}
	assert.Equal(t, address1, ConfigValueLookup(burntContract, 22640000))
	assert.Equal(t, address1, ConfigValueLookup(burntContract, 22640000+1))
	assert.Equal(t, address1, ConfigValueLookup(burntContract, 41874000-1))
	assert.Equal(t, address2, ConfigValueLookup(burntContract, 41874000))
	assert.Equal(t, address2, ConfigValueLookup(burntContract, 41874000+1))
}

func TestNilBlobSchedule(t *testing.T) {
	var b *BlobSchedule

	// Original EIP-4844 values
	isPrague := false
	assert.Equal(t, uint64(3), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(6), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(3338477), b.BaseFeeUpdateFraction(isPrague))

	// EIP-7691: Blob throughput increase
	isPrague = true
	assert.Equal(t, uint64(6), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(9), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(5007716), b.BaseFeeUpdateFraction(isPrague))
}
