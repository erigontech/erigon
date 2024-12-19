package misc

import (
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

func DequeueConsolidationRequests7251(syscall consensus.SystemCall) *types.FlatRequest {
	res, err := syscall(params.ConsolidationRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to ConsolidationRequestAddress", "err", err)
		return nil
	}
	if res == nil {
		res = make([]byte, 0)
	}
	// Just append the contract outputs as the encoded request data
	return &types.FlatRequest{Type: types.ConsolidationRequestType, RequestData: res}
}
