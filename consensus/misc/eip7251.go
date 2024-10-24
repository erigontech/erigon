package misc

import (
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

const ConsolidationRequestDataLen = 116

func DequeueConsolidationRequests7251(syscall consensus.SystemCall) types.Requests {
	res, err := syscall(params.ConsolidationRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to ConsolidationRequestAddress", "err", err)
		return nil
	}
	// Just append the contract outputs as the encoded request data
	var reqs types.Requests
	for i := 0; i <= len(res)-ConsolidationRequestDataLen; i += ConsolidationRequestDataLen {
		wr := &types.ConsolidationRequest{
			RequestData: [ConsolidationRequestDataLen]byte(res[i : i+ConsolidationRequestDataLen]),
		}
		reqs = append(reqs, wr)
	}
	return reqs
}
