package misc

import (
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func DequeueWithdrawalRequests7002(syscall consensus.SystemCall) *types.FlatRequest {
	res, err := syscall(params.WithdrawalRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to WithdrawalRequestAddress", "err", err)
		return nil
	}
	// Just append the contract outputs
	return &types.FlatRequest{Type: types.WithdrawalRequestType, RequestData: res}
}
