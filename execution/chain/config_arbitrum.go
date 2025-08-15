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

package chain

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
)

func ArbitrumOneParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       ArbosVersion_6,
		InitialChainOwner:         common.HexToAddress("0xd345e41ae2cb00311956aa7109fc801ae8c81a52"),
	}
}

func ArbitrumNovaParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       ArbosVersion_1,
		InitialChainOwner:         common.HexToAddress("0x9C040726F2A657226Ed95712245DeE84b650A1b5"),
	}
}

func ArbitrumRollupGoerliTestnetParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       ArbosVersion_2,
		InitialChainOwner:         common.HexToAddress("0x186B56023d42B2B4E7616589a5C62EEf5FCa21DD"),
	}
}

func ArbitrumDevTestParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     true,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       ArbosVersion_32,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumDevTestDASParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     true,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       ArbosVersion_32,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumAnytrustGoerliTestnetParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       ArbosVersion_2,
		InitialChainOwner:         common.HexToAddress("0x186B56023d42B2B4E7616589a5C62EEf5FCa21DD"),
	}
}

func DisableArbitrumParams() ArbitrumChainParams {
	return ArbitrumChainParams{
		EnableArbOS:               false,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       ArbosVersion_0,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumOneChainConfig() *Config {
	return &Config{
		ChainID:               big.NewInt(42161),
		HomesteadBlock:        big.NewInt(0),
		DAOForkBlock:          nil,
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArbitrumChainParams:   ArbitrumOneParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumNovaChainConfig() *Config {
	return &Config{
		ChainID:               big.NewInt(42170),
		HomesteadBlock:        big.NewInt(0),
		DAOForkBlock:          nil,
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArbitrumChainParams:   ArbitrumNovaParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumRollupGoerliTestnetChainConfig() *Config {
	return &Config{
		ChainID:               big.NewInt(421613),
		HomesteadBlock:        big.NewInt(0),
		DAOForkBlock:          nil,
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArbitrumChainParams:   ArbitrumRollupGoerliTestnetParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumDevTestChainConfig() *Config {
	return &Config{
		ChainID:               big.NewInt(412346),
		HomesteadBlock:        big.NewInt(0),
		DAOForkBlock:          nil,
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArbitrumChainParams:   ArbitrumDevTestParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumDevTestDASChainConfig() *Config {
	return &Config{
		ChainID:               big.NewInt(412347),
		HomesteadBlock:        big.NewInt(0),
		DAOForkBlock:          nil,
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArbitrumChainParams:   ArbitrumDevTestDASParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumAnytrustGoerliTestnetChainConfig() *Config {
	return &Config{
		ChainID:               big.NewInt(421703),
		HomesteadBlock:        big.NewInt(0),
		DAOForkBlock:          nil,
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArbitrumChainParams:   ArbitrumAnytrustGoerliTestnetParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

var ArbitrumSupportedChainConfigs = []*Config{
	ArbitrumOneChainConfig(),
	ArbitrumNovaChainConfig(),
	ArbitrumRollupGoerliTestnetChainConfig(),
	ArbitrumDevTestChainConfig(),
	ArbitrumDevTestDASChainConfig(),
	ArbitrumAnytrustGoerliTestnetChainConfig(),
}

// AllEthashProtocolChanges contains every protocol change (EIPs) introduced
// and accepted by the Ethereum core developers into the Ethash consensus.
var AllEthashProtocolChanges = &Config{
	ChainID:                       big.NewInt(1337),
	HomesteadBlock:                big.NewInt(0),
	DAOForkBlock:                  nil,
	TangerineWhistleBlock:         big.NewInt(0),
	SpuriousDragonBlock:           big.NewInt(0),
	ByzantiumBlock:                big.NewInt(0),
	ConstantinopleBlock:           big.NewInt(0),
	PetersburgBlock:               big.NewInt(0),
	IstanbulBlock:                 big.NewInt(0),
	MuirGlacierBlock:              big.NewInt(0),
	BerlinBlock:                   big.NewInt(0),
	LondonBlock:                   big.NewInt(0),
	ArrowGlacierBlock:             big.NewInt(0),
	GrayGlacierBlock:              big.NewInt(0),
	MergeNetsplitBlock:            nil,
	ShanghaiTime:                  nil,
	CancunTime:                    nil,
	PragueTime:                    nil,
	TerminalTotalDifficulty:       nil,
	TerminalTotalDifficultyPassed: true,
	Ethash:                        new(EthashConfig),
	Clique:                        nil,
	ArbitrumChainParams:           DisableArbitrumParams(),
}
