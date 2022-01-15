package systemcontracts

import "github.com/ledgerwatch/erigon/common"

var (
	// genesis contracts
	ValidatorContract          = common.HexToAddress("0x0000000000000000000000000000000000001000")
	SlashContract              = common.HexToAddress("0x0000000000000000000000000000000000001001")
	SystemRewardContract       = common.HexToAddress("0x0000000000000000000000000000000000001002")
	LightClientContract        = common.HexToAddress("0x0000000000000000000000000000000000001003")
	TokenHubContract           = common.HexToAddress("0x0000000000000000000000000000000000001004")
	RelayerIncentivizeContract = common.HexToAddress("0x0000000000000000000000000000000000001005")
	RelayerHubContract         = common.HexToAddress("0x0000000000000000000000000000000000001006")
	GovHubContract             = common.HexToAddress("0x0000000000000000000000000000000000001007")
	TokenManagerContract       = common.HexToAddress("0x0000000000000000000000000000000000001008")
	CrossChainContract         = common.HexToAddress("0x0000000000000000000000000000000000002000")
)
