package misc

import (
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

func DequeueWithdrawalRequests7002(syscall consensus.SystemCall) *types.FlatRequest {
	res, err := syscall(params.WithdrawalRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to WithdrawalRequestAddress", "err", err)
		return nil
	}
	if res != nil {
		// Just append the contract output
		return &types.FlatRequest{Type: types.WithdrawalRequestType, RequestData: res}
	}
	return nil
}
