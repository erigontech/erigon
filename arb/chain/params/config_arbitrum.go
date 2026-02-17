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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/arb/chain/types"
	"github.com/erigontech/erigon/arb/osver"
	"github.com/erigontech/erigon/execution/chain"

	"math/big"
)

func ArbitrumOneParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		ParentChainID:             1,
		ParentChainIsArbitrum:     false,
		ChainName:                 "arb1",
		SequencerURL:              "https://arb1-sequencer.arbitrum.io/rpc",
		FeedURL:                   "wss://arb1-feed.arbitrum.io/feed",
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       osver.ArbosVersion_6,
		InitialChainOwner:         common.HexToAddress("0xd345e41ae2cb00311956aa7109fc801ae8c81a52"),
		GenesisBlockNum:           22207817,
		GenesisTxNum:              69372193, // non-canonical (E3) txnumber of first txn in given block

		Rollup: types.ArbRollupConfig{
			Bridge:                 "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a",
			Inbox:                  "0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f",
			SequencerInbox:         "0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6",
			Rollup:                 "0x5ef0d09d1e6204141b4d37530808ed19f60fba35",
			ValidatorUtils:         "0x9e40625f52829cf04bc4839f186d621ee33b0e67",
			ValidatorWalletCreator: "0x960953f7c69cd2bc2322db9223a815c680ccc7ea",
			StakeToken:             "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
			DeployedAt:             15411056,
		},
	}
}

func ArbitrumNovaParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       osver.ArbosVersion_1,
		InitialChainOwner:         common.HexToAddress("0x9C040726F2A657226Ed95712245DeE84b650A1b5"),
	}
}

func ArbitrumRollupGoerliTestnetParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       osver.ArbosVersion_2,
		InitialChainOwner:         common.HexToAddress("0x186B56023d42B2B4E7616589a5C62EEf5FCa21DD"),
	}
}

func ArbitrumDevTestParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     true,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       osver.ArbosVersion_32,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumDevTestDASParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     true,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       osver.ArbosVersion_32,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumAnytrustGoerliTestnetParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: true,
		InitialArbOSVersion:       osver.ArbosVersion_2,
		InitialChainOwner:         common.HexToAddress("0x186B56023d42B2B4E7616589a5C62EEf5FCa21DD"),
	}
}

func DisableArbitrumParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		EnableArbOS:               false,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       osver.ArbosVersion_0,
		InitialChainOwner:         common.Address{},
	}
}

func ArbitrumOneChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:             big.NewInt(42161),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumOneParams(),
		Clique: &chain.CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumNovaChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:             big.NewInt(42170),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumNovaParams(),
		Clique: &chain.CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumRollupGoerliTestnetChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:             big.NewInt(421613),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumRollupGoerliTestnetParams(),
		Clique: &chain.CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumDevTestChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:             big.NewInt(412346),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumDevTestParams(),
		Clique: &chain.CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumDevTestDASChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:             big.NewInt(412347),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumDevTestDASParams(),
		Clique: &chain.CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumAnytrustGoerliTestnetChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:             big.NewInt(421703),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumAnytrustGoerliTestnetParams(),
		Clique: &chain.CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

var ArbitrumSupportedChainConfigs = []*chain.Config{
	ArbitrumOneChainConfig(),
	ArbitrumNovaChainConfig(),
	ArbitrumRollupGoerliTestnetChainConfig(),
	ArbitrumDevTestChainConfig(),
	ArbitrumDevTestDASChainConfig(),
	ArbitrumAnytrustGoerliTestnetChainConfig(),
}

// AllEthashProtocolChanges contains every protocol change (EIPs) introduced
// and accepted by the Ethereum core developers into the Ethash consensus.
var AllEthashProtocolChanges = &chain.Config{
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
	Ethash:                        new(chain.EthashConfig),
	Clique:                        nil,
	ArbitrumChainParams:           DisableArbitrumParams(),
}
