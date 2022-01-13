package systemcontracts

import "github.com/ledgerwatch/erigon/common"

var (
	// genesis contracts
	ValidatorContract          = common.Address{0x10, 0x0}
	SlashContract              = common.Address{0x10, 0x1}
	SystemRewardContract       = common.Address{0x10, 0x2}
	LightClientContract        = common.Address{0x10, 0x3}
	TokenHubContract           = common.Address{0x10, 0x4}
	RelayerIncentivizeContract = common.Address{0x10, 0x5}
	RelayerHubContract         = common.Address{0x10, 0x6}
	GovHubContract             = common.Address{0x10, 0x7}
	TokenManagerContract       = common.Address{0x10, 0x8}
	CrossChainContract         = common.Address{0x20, 0x0}
)
