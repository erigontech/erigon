package tests

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
)

type EngineTest struct {
	config EngineTestJson
}

type EngineTestJson struct {
	Blocks     []btBlock             `json:"blocks"`
	Genesis    btHeader              `json:"genesisBlockHeader"`
	Pre        types.GenesisAlloc    `json:"pre"`
	Post       types.GenesisAlloc    `json:"postState"`
	BestBlock  common.UnprefixedHash `json:"lastblockhash"`
	Network    string                `json:"network"`
	SealEngine string                `json:"sealEngine"`
}
