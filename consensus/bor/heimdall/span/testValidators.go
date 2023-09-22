package span

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/params/networkname"
)

// NetworkNameVals is a map of network name to validator set for tests/devnets
var NetworkNameVals = make(map[string][]*valset.Validator)

// Validator set for bor e2e test chain with 2 validator configuration
var BorE2ETestChain2Valset = []*valset.Validator{
	{
		ID:               1,
		Address:          common.HexToAddress("71562b71999873DB5b286dF957af199Ec94617F7"),
		VotingPower:      1000,
		ProposerPriority: 1,
	},
	{
		ID:               2,
		Address:          common.HexToAddress("9fB29AAc15b9A4B7F17c3385939b007540f4d791"),
		VotingPower:      1000,
		ProposerPriority: 2,
	},
}

// Validator set for bor devnet-chain with 1 validator configuration
var BorDevnetChainVals = []*valset.Validator{
	{
		ID:               1,
		Address:          common.HexToAddress("0x67b1d87101671b127f5f8714789C7192f7ad340e"),
		VotingPower:      1000,
		ProposerPriority: 1,
	},
}

func init() {
	NetworkNameVals[networkname.BorE2ETestChain2ValName] = BorE2ETestChain2Valset
	NetworkNameVals[networkname.BorDevnetChainName] = BorDevnetChainVals
}
