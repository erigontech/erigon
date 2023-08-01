package misc

import (
	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func ApplyBeaconRootEip4788(chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState) {
	historyStorageAddress := libcommon.BytesToAddress(params.HistoryStorageAddress)
	historicalRootsModulus := params.HistoricalRootsModulus
	timestampReduced := header.Time % historicalRootsModulus
	timestampExtended := timestampReduced + historicalRootsModulus
	timestampIndex := libcommon.BytesToHash((uint256.NewInt(timestampReduced)).Bytes())
	rootIndex := libcommon.BytesToHash(uint256.NewInt(timestampExtended).Bytes())
	parentBeaconBlockRootInt := *uint256.NewInt(0).SetBytes(header.ParentBeaconBlockRoot.Bytes())
	state.SetState(historyStorageAddress, &timestampIndex, *uint256.NewInt(header.Time))
	state.SetState(historyStorageAddress, &rootIndex, parentBeaconBlockRootInt)
}
