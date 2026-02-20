package types

import "github.com/erigontech/erigon/common"

type ArbRollupConfig struct {
	Bridge                 string `json:"bridge"`
	Inbox                  string `json:"inbox"`
	SequencerInbox         string `json:"sequencer-inbox"`
	Rollup                 string `json:"rollup"`
	ValidatorUtils         string `json:"validator-utils"`
	ValidatorWalletCreator string `json:"validator-wallet-creator"`
	StakeToken             string `json:"stake-token"`
	DeployedAt             int    `json:"deployed-at"`
}

type ArbitrumChainParams struct {
	ParentChainID         int    `json:"parent-chain-id"`
	ParentChainIsArbitrum bool   `json:"parent-chain-is-arbitrum"`
	ChainName             string `json:"chain-name"`
	SequencerURL          string `json:"sequencer-url"`
	FeedURL               string `json:"feed-url"`

	EnableArbOS               bool           `json:"EnableArbOS"`
	AllowDebugPrecompiles     bool           `json:"AllowDebugPrecompiles"`
	DataAvailabilityCommittee bool           `json:"DataAvailabilityCommittee"`
	InitialArbOSVersion       uint64         `json:"InitialArbOSVersion"`
	InitialChainOwner         common.Address `json:"InitialChainOwner"`
	GenesisBlockNum           uint64         `json:"GenesisBlockNum"`

	MaxCodeSize     uint64 `json:"MaxCodeSize,omitempty"`     // Maximum bytecode to permit for a contract. 0 value implies params.DefaultMaxCodeSize
	MaxInitCodeSize uint64 `json:"MaxInitCodeSize,omitempty"` // Maximum initcode to permit in a creation transaction and create instructions. 0 value implies params.DefaultMaxInitCodeSize

	Rollup ArbRollupConfig `json:"rollup"`
}
