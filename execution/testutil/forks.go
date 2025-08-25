// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package testutil

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/jinzhu/copier"
)

// See https://github.com/ethereum/execution-spec-tests/pull/2050
var blobSchedule = map[string]*params.BlobConfig{
	"bpo1": {
		Target:                9,
		Max:                   14,
		BaseFeeUpdateFraction: 8832827,
	},
	"bpo2": {
		Target:                14,
		Max:                   21,
		BaseFeeUpdateFraction: 13739630,
	},
	"bpo3": {
		Target:                21,
		Max:                   32,
		BaseFeeUpdateFraction: 20609697,
	},
	"bpo4": {
		Target:                14,
		Max:                   21,
		BaseFeeUpdateFraction: 13739630,
	},
}

// Forks table defines supported forks and their chain config.
var Forks = map[string]*chain.Config{}

func init() {
	c := &chain.Config{ChainID: big.NewInt(1)}
	Forks["Frontier"] = c

	c = configCopy(c)
	c.HomesteadBlock = big.NewInt(5)
	Forks["FrontierToHomesteadAt5"] = c

	c = configCopy(c)
	c.HomesteadBlock = big.NewInt(0)
	Forks["Homestead"] = c

	c = configCopy(c)
	c.DAOForkBlock = big.NewInt(5)
	Forks["HomesteadToDaoAt5"] = c

	c = configCopy(c)
	c.DAOForkBlock = nil
	c.TangerineWhistleBlock = big.NewInt(5)
	Forks["HomesteadToEIP150At5"] = c

	c = configCopy(c)
	c.TangerineWhistleBlock = big.NewInt(0)
	Forks["EIP150"] = c

	c = configCopy(c)
	c.SpuriousDragonBlock = big.NewInt(0)
	Forks["EIP158"] = c

	c = configCopy(c)
	c.ByzantiumBlock = big.NewInt(5)
	Forks["EIP158ToByzantiumAt5"] = c

	c = configCopy(c)
	c.ByzantiumBlock = big.NewInt(0)
	Forks["Byzantium"] = c

	c = configCopy(c)
	c.ConstantinopleBlock = big.NewInt(5)
	Forks["ByzantiumToConstantinopleAt5"] = c

	c = configCopy(c)
	c.PetersburgBlock = big.NewInt(5)
	Forks["ByzantiumToConstantinopleFixAt5"] = c

	c = configCopy(c)
	c.ConstantinopleBlock = big.NewInt(0)
	c.PetersburgBlock = nil
	Forks["Constantinople"] = c

	c = configCopy(c)
	c.PetersburgBlock = big.NewInt(0)
	Forks["ConstantinopleFix"] = c

	c = configCopy(c)
	c.IstanbulBlock = big.NewInt(5)
	Forks["ConstantinopleFixToIstanbulAt5"] = c

	c = configCopy(c)
	c.IstanbulBlock = big.NewInt(0)
	Forks["Istanbul"] = c

	c = configCopy(c)
	c.MuirGlacierBlock = big.NewInt(0)
	Forks["EIP2384"] = c

	c = configCopy(c)
	c.BerlinBlock = big.NewInt(0)
	Forks["Berlin"] = c

	c = configCopy(c)
	c.LondonBlock = big.NewInt(5)
	Forks["BerlinToLondonAt5"] = c

	c = configCopy(c)
	c.LondonBlock = big.NewInt(0)
	Forks["London"] = c

	c = configCopy(c)
	c.ArrowGlacierBlock = big.NewInt(0)
	Forks["ArrowGlacier"] = c

	c = configCopy(c)
	c.TerminalTotalDifficulty = big.NewInt(0xC00000)
	Forks["ArrowGlacierToParisAtDiffC0000"] = c

	c = configCopy(c)
	c.TerminalTotalDifficulty = nil
	c.GrayGlacierBlock = big.NewInt(0)
	Forks["GrayGlacier"] = c

	c = configCopy(c)
	c.TerminalTotalDifficulty = big.NewInt(0)
	Forks["Merge"] = c
	Forks["Paris"] = c

	c = configCopy(c)
	c.TerminalTotalDifficultyPassed = true
	c.ShanghaiTime = big.NewInt(15_000)
	Forks["ParisToShanghaiAtTime15k"] = c

	c = configCopy(c)
	c.ShanghaiTime = big.NewInt(0)
	Forks["Shanghai"] = c

	c = configCopy(c)
	c.CancunTime = big.NewInt(15_000)
	Forks["ShanghaiToCancunAtTime15k"] = c

	c = configCopy(c)
	c.CancunTime = big.NewInt(0)
	Forks["Cancun"] = c

	c = configCopy(c)
	c.PragueTime = big.NewInt(15_000)
	c.DepositContract = common.HexToAddress("0x00000000219ab540356cBB839Cbe05303d7705Fa")
	Forks["CancunToPragueAtTime15k"] = c

	c = configCopy(c)
	c.PragueTime = big.NewInt(0)
	Forks["Prague"] = c

	c = configCopy(c)
	c.OsakaTime = big.NewInt(15_000)
	Forks["PragueToOsakaAtTime15k"] = c

	c = configCopy(c)
	c.OsakaTime = big.NewInt(0)
	Forks["Osaka"] = c

	c = configCopy(c)
	c.BlobSchedule = blobSchedule
	c.Bpo1Time = big.NewInt(15_000)
	Forks["OsakaToBPO1AtTime15k"] = c

	c = configCopy(c)
	c.Bpo1Time = big.NewInt(0)
	Forks["BPO1"] = c

	c = configCopy(c)
	c.Bpo2Time = big.NewInt(15_000)
	Forks["BPO1ToBPO2AtTime15k"] = c

	c = configCopy(c)
	c.Bpo2Time = big.NewInt(0)
	Forks["BPO2"] = c

	c = configCopy(c)
	c.Bpo3Time = big.NewInt(15_000)
	Forks["BPO2ToBPO3AtTime15k"] = c

	c = configCopy(c)
	c.Bpo3Time = big.NewInt(0)
	Forks["BPO3"] = c

	c = configCopy(c)
	c.Bpo4Time = big.NewInt(15_000)
	Forks["BPO3ToBPO4AtTime15k"] = c

	c = configCopy(c)
	c.Bpo4Time = big.NewInt(0)
	Forks["BPO4"] = c
}

func configCopy(c *chain.Config) *chain.Config {
	cpy := new(chain.Config)
	copier.Copy(cpy, c)
	return cpy
}

// Returns the set of defined fork names
func AvailableForks() []string {
	var availableForks []string //nolint:prealloc
	for k := range Forks {
		availableForks = append(availableForks, k)
	}
	sort.Strings(availableForks)
	return availableForks
}

// UnsupportedForkError is returned when a test requests a fork that isn't implemented.
type UnsupportedForkError struct {
	Name string
}

func (e UnsupportedForkError) Error() string {
	return fmt.Sprintf("unsupported fork %q", e.Name)
}
