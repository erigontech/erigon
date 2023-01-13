package systemcontracts

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

var (
	// genesis contracts
	ValidatorContract          = libcommon.HexToAddress("0x0000000000000000000000000000000000001000")
	SlashContract              = libcommon.HexToAddress("0x0000000000000000000000000000000000001001")
	SystemRewardContract       = libcommon.HexToAddress("0x0000000000000000000000000000000000001002")
	LightClientContract        = libcommon.HexToAddress("0x0000000000000000000000000000000000001003")
	TokenHubContract           = libcommon.HexToAddress("0x0000000000000000000000000000000000001004")
	RelayerIncentivizeContract = libcommon.HexToAddress("0x0000000000000000000000000000000000001005")
	RelayerHubContract         = libcommon.HexToAddress("0x0000000000000000000000000000000000001006")
	GovHubContract             = libcommon.HexToAddress("0x0000000000000000000000000000000000001007")
	TokenManagerContract       = libcommon.HexToAddress("0x0000000000000000000000000000000000001008")
	CrossChainContract         = libcommon.HexToAddress("0x0000000000000000000000000000000000002000")
	StakingContract            = libcommon.HexToAddress("0x0000000000000000000000000000000000002001")
)
