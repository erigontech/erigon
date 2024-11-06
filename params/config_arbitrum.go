// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

const ArbosVersion_FixRedeemGas = uint64(11)
const ArbosVersion_Stylus = uint64(30)
const ArbosVersion_StylusFixes = uint64(31)
const ArbosVersion_StylusChargingFixes = uint64(32)

type ArbitrumChainParams struct {
	EnableArbOS               bool
	AllowDebugPrecompiles     bool
	DataAvailabilityCommittee bool
	InitialArbOSVersion       uint64
	InitialChainOwner         common.Address
	GenesisBlockNum           uint64
	MaxCodeSize               uint64 `json:"MaxCodeSize,omitempty"`     // Maximum bytecode to permit for a contract. 0 value implies params.DefaultMaxCodeSize
	MaxInitCodeSize           uint64 `json:"MaxInitCodeSize,omitempty"` // Maximum initcode to permit in a creation transaction and create instructions. 0 value implies params.DefaultMaxInitCodeSize
}

func (c *ChainConfig) IsArbitrum() bool {
	return c.ArbitrumChainParams.EnableArbOS
}

func (c *ChainConfig) IsArbitrumNitro(num *big.Int) bool {
	return c.IsArbitrum() && isBlockForked(new(big.Int).SetUint64(c.ArbitrumChainParams.GenesisBlockNum), num)
}

func (c *ChainConfig) MaxCodeSize() uint64 {
	if c.ArbitrumChainParams.MaxCodeSize == 0 {
		return DefaultMaxCodeSize
	}
	return c.ArbitrumChainParams.MaxCodeSize
}

func (c *ChainConfig) MaxInitCodeSize() uint64 {
	if c.ArbitrumChainParams.MaxInitCodeSize == 0 {
		return c.MaxCodeSize() * 2
	}
	return c.ArbitrumChainParams.MaxInitCodeSize
}

func (c *ChainConfig) DebugMode() bool {
	return c.ArbitrumChainParams.AllowDebugPrecompiles
}

func (c *ChainConfig) checkArbitrumCompatible(newcfg *ChainConfig, head *big.Int) *ConfigCompatError {
	if c.IsArbitrum() != newcfg.IsArbitrum() {
		// This difference applies to the entire chain, so report that the genesis block is where the difference appears.
		return newBlockCompatError("isArbitrum", common.Big0, common.Big0)
	}
	if !c.IsArbitrum() {
		return nil
	}
	cArb := &c.ArbitrumChainParams
	newArb := &newcfg.ArbitrumChainParams
	if cArb.GenesisBlockNum != newArb.GenesisBlockNum {
		return newBlockCompatError("genesisblocknum", new(big.Int).SetUint64(cArb.GenesisBlockNum), new(big.Int).SetUint64(newArb.GenesisBlockNum))
	}
	return nil
}

func ArbitrumOneParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       6,
		InitialChainOwner:         common.HexToAddress("0xd345e41ae2cb00311956aa7109fc801ae8c81a52"),
	}
}

func ArbitrumNovaParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       1,
		InitialChainOwner:         common.HexToAddress("0x9C040726F2A657226Ed95712245DeE84b650A1b5"),
	}
}

func ArbitrumRollupGoerliTestnetParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       2,
		InitialChainOwner:         common.HexToAddress("0x186B56023d42B2B4E7616589a5C62EEf5FCa21DD"),
	}
}

func ArbitrumDevTestParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     true,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       32,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumDevTestDASParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     true,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       32,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumAnytrustGoerliTestnetParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       2,
		InitialChainOwner:         common.HexToAddress("0x186B56023d42B2B4E7616589a5C62EEf5FCa21DD"),
	}
}

func DisableArbitrumParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               false,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       0,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumOneChainConfig() *ChainConfig {
	return &ChainConfig{
		ChainID:             big.NewInt(42161),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		DAOForkSupport:      true,
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumOneParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumNovaChainConfig() *ChainConfig {
	return &ChainConfig{
		ChainID:             big.NewInt(42170),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		DAOForkSupport:      true,
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumNovaParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumRollupGoerliTestnetChainConfig() *ChainConfig {
	return &ChainConfig{
		ChainID:             big.NewInt(421613),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		DAOForkSupport:      true,
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumRollupGoerliTestnetParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumDevTestChainConfig() *ChainConfig {
	return &ChainConfig{
		ChainID:             big.NewInt(412346),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		DAOForkSupport:      true,
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumDevTestParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumDevTestDASChainConfig() *ChainConfig {
	return &ChainConfig{
		ChainID:             big.NewInt(412347),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		DAOForkSupport:      true,
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumDevTestDASParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumAnytrustGoerliTestnetChainConfig() *ChainConfig {
	return &ChainConfig{
		ChainID:             big.NewInt(421703),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		DAOForkSupport:      true,
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumAnytrustGoerliTestnetParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

var ArbitrumSupportedChainConfigs = []*ChainConfig{
	ArbitrumOneChainConfig(),
	ArbitrumNovaChainConfig(),
	ArbitrumRollupGoerliTestnetChainConfig(),
	ArbitrumDevTestChainConfig(),
	ArbitrumDevTestDASChainConfig(),
	ArbitrumAnytrustGoerliTestnetChainConfig(),
}
