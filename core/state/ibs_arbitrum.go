package state

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

func NewArbitrum(ibs *IntraBlockState) IntraBlockStateArbitrum {
	ibs.arbExtraData = &ArbitrumExtraData{
		unexpectedBalanceDelta: new(uint256.Int),
		userWasms:              map[libcommon.Hash]ActivatedWasm{},
		activatedWasms:         map[libcommon.Hash]ActivatedWasm{},
		recentWasms:            NewRecentWasms(),
	}
	return ibs // TODO
}
