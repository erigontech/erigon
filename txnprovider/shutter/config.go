package shutter

import (
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon/params"
)

type Config struct {
	Enabled                          bool     `json:"-"`
	InstanceId                       uint64   `json:"instanceId"`
	SequencerContractAddress         string   `json:"sequencerContractAddress"`
	ValidatorRegistryContractAddress string   `json:"validatorRegistryContractAddress"`
	KeyBroadcastContractAddress      string   `json:"keyBroadcastContractAddress"`
	KeyperSetManagerContractAddress  string   `json:"keyperSetManagerContractAddress"`
	KeyperBootnodes                  []string `json:"keyperBootnodes"`
}

func ConfigByChainName(chainName string) Config {
	switch chainName {
	case networkname.Chiado:
		return chiadoConfig
	case networkname.Gnosis:
		return gnosisConfig
	default:
		panic("missing shutter config for chain: " + chainName)
	}
}

var (
	chiadoConfig = Config{
		Enabled:                          true,
		InstanceId:                       params.ChiadoChainConfig.ChainID.Uint64(),
		SequencerContractAddress:         "0x2aD8E2feB0ED5b2EC8e700edB725f120576994ed",
		ValidatorRegistryContractAddress: "0xa9289A3Dd14FEBe10611119bE81E5d35eAaC3084",
		KeyBroadcastContractAddress:      "0x9D31865BEffcE842FBd36CDA587aDDA8bef804B7",
		KeyperSetManagerContractAddress:  "0xC4DE9FAf4ec882b33dA0162CBE628B0D8205D0c0",
		KeyperBootnodes: []string{
			"/ip4/167.99.177.227/tcp/23005/p2p/12D3KooWSdm5guPBdn8DSaBphVBzUUgPLg9sZLnazEUrcbtLy254",
			"/ip4/159.89.15.119/tcp/23005/p2p/12D3KooWPP6bp2PJQR8rUvG1SD4qNH4WFrKve6DMgWThyKxwNbbH",
		},
	}

	gnosisConfig = Config{
		Enabled:                          true,
		InstanceId:                       params.GnosisChainConfig.ChainID.Uint64(),
		SequencerContractAddress:         "0xc5C4b277277A1A8401E0F039dfC49151bA64DC2E",
		ValidatorRegistryContractAddress: "0xefCC23E71f6bA9B22C4D28F7588141d44496A6D6",
		KeyBroadcastContractAddress:      "0x626dB87f9a9aC47070016A50e802dd5974341301",
		KeyperSetManagerContractAddress:  "0x7C2337f9bFce19d8970661DA50dE8DD7d3D34abb",
		KeyperBootnodes: []string{
			"/ip4/167.99.177.227/tcp/23003/p2p/12D3KooWD35AESYCttDEi3J5WnQdTFuM5JNtmuXEb1x4eQ28gb1s",
			"/ip4/159.89.15.119/tcp/23003/p2p/12D3KooWRzAhgPA16DiBQhiuYoasYzJaQSAbtc5i5FvgTi9ZDQtS",
		},
	}
)
