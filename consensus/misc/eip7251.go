package misc

import (
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func DequeueConsolidationRequests7251(syscall consensus.SystemCall) *types.FlatRequest {
	res, err := syscall(params.ConsolidationRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to ConsolidationRequestAddress", "err", err)
		return nil
	}
	// Just append the contract outputs as the encoded request data
	return &types.FlatRequest{Type: types.ConsolidationRequestType, RequestData: res}
}
