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
	"math/big"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
)

func TestConfigValueLookup(t *testing.T) {
	foo := map[uint64]string{5: "A", 10: "B", 20: "C"}
	assert.Equal(t, "", ConfigValueLookup(foo, 0))
	assert.Equal(t, "", ConfigValueLookup(foo, 4))
	assert.Equal(t, "A", ConfigValueLookup(foo, 5))
	assert.Equal(t, "A", ConfigValueLookup(foo, 9))
	assert.Equal(t, "B", ConfigValueLookup(foo, 10))
	assert.Equal(t, "B", ConfigValueLookup(foo, 11))
	assert.Equal(t, "B", ConfigValueLookup(foo, 15))
	assert.Equal(t, "B", ConfigValueLookup(foo, 19))
	assert.Equal(t, "C", ConfigValueLookup(foo, 20))
	assert.Equal(t, "C", ConfigValueLookup(foo, 21))
	assert.Equal(t, "C", ConfigValueLookup(foo, 100))
	assert.Equal(t, "C", ConfigValueLookup(foo, 1_000_000_000_000))

	backupMultiplier := map[uint64]uint64{
		0:        2,
		25275000: 5,
		29638656: 2,
	}
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 0))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 1))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 25275000-1))
	assert.Equal(t, uint64(5), ConfigValueLookup(backupMultiplier, 25275000))
	assert.Equal(t, uint64(5), ConfigValueLookup(backupMultiplier, 25275000+1))
	assert.Equal(t, uint64(5), ConfigValueLookup(backupMultiplier, 29638656-1))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 29638656))
	assert.Equal(t, uint64(2), ConfigValueLookup(backupMultiplier, 29638656+1))

	config := map[uint64]uint64{
		0:         1,
		90000000:  2,
		100000000: 3,
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

	burntContract := map[uint64]common.Address{
		22640000: address1,
		41874000: address2,
	}
	assert.Equal(t, common.Address{}, ConfigValueLookup(burntContract, 10000000))
	assert.Equal(t, address1, ConfigValueLookup(burntContract, 22640000))
	assert.Equal(t, address1, ConfigValueLookup(burntContract, 22640000+1))
	assert.Equal(t, address1, ConfigValueLookup(burntContract, 41874000-1))
	assert.Equal(t, address2, ConfigValueLookup(burntContract, 41874000))
	assert.Equal(t, address2, ConfigValueLookup(burntContract, 41874000+1))
}

func TestNilBlobSchedule(t *testing.T) {
	var c Config
	c.CancunTime = big.NewInt(1)
	c.PragueTime = big.NewInt(2)

	// Everything should be 0 before Cancun
	assert.Equal(t, uint64(0), c.GetTargetBlobGasPerBlock(0))
	assert.Equal(t, uint64(0), c.GetMaxBlobsPerBlock(0))
	assert.Equal(t, uint64(0), c.GetBlobGasPriceUpdateFraction(0))

	// Original EIP-4844 values
	assert.Equal(t, 3*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(1))
	assert.Equal(t, uint64(6), c.GetMaxBlobsPerBlock(1))
	assert.Equal(t, uint64(3338477), c.GetBlobGasPriceUpdateFraction(1))

	// EIP-7691: Blob throughput increase
	assert.Equal(t, 6*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(2))
	assert.Equal(t, uint64(9), c.GetMaxBlobsPerBlock(2))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(2))
}

// EIP-7892
func TestBlobParameterOnlyHardforks(t *testing.T) {
	cancunTime := uint64(1710338135)
	pragueTime := uint64(1746612311)
	timeA := uint64(1775065900)
	timeB := uint64(1785952240)

	var c Config
	c.CancunTime = big.NewInt(int64(cancunTime))
	c.PragueTime = big.NewInt(int64(pragueTime))

	c.BlobSchedule = map[string]*params.BlobConfig{
		"cancun": {
			Target:                3,
			Max:                   6,
			BaseFeeUpdateFraction: 3338477,
		},
		"prague": {
			Target:                6,
			Max:                   9,
			BaseFeeUpdateFraction: 5007716,
		},
		strconv.FormatUint(timeA, 10): {
			Target:                24,
			Max:                   48,
			BaseFeeUpdateFraction: 5007716,
		},
		strconv.FormatUint(timeB, 10): {
			Target:                36,
			Max:                   56,
			BaseFeeUpdateFraction: 5007716,
		},
	}

	time := uint64(0)
	assert.Equal(t, uint64(0), c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(0), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(0), c.GetBlobGasPriceUpdateFraction(time))

	time = cancunTime
	assert.Equal(t, 3*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(6), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(3338477), c.GetBlobGasPriceUpdateFraction(time))

	time = (cancunTime + pragueTime) / 2
	assert.Equal(t, 3*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(6), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(3338477), c.GetBlobGasPriceUpdateFraction(time))

	time = pragueTime
	assert.Equal(t, 6*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(9), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))

	time = (pragueTime + timeA) / 2
	assert.Equal(t, 6*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(9), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))

	time = timeA
	assert.Equal(t, 24*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(48), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))

	time = (timeA + timeB) / 2
	assert.Equal(t, 24*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(48), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))

	time = timeB
	assert.Equal(t, 36*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(56), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))

	time = timeB * 2
	assert.Equal(t, 36*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(56), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))
}

func TestBlobParameterInactiveHardfork(t *testing.T) {
	cancunTime := uint64(1710338135)
	pragueTime := uint64(1746612311)

	var c Config
	c.CancunTime = big.NewInt(int64(cancunTime))
	c.PragueTime = big.NewInt(int64(pragueTime))
	// Osaka is not activated yet

	c.BlobSchedule = map[string]*params.BlobConfig{
		"cancun": {
			Target:                3,
			Max:                   6,
			BaseFeeUpdateFraction: 3338477,
		},
		"prague": {
			Target:                6,
			Max:                   9,
			BaseFeeUpdateFraction: 5007716,
		},
		"osaka": {
			Target:                12,
			Max:                   24,
			BaseFeeUpdateFraction: 3338477,
		},
	}

	time := pragueTime * 2
	assert.Equal(t, 6*params.BlobGasPerBlob, c.GetTargetBlobGasPerBlock(time))
	assert.Equal(t, uint64(9), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))
}
