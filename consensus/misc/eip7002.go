package misc

import (
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

// Configuration related to EIP-7002
// (May have to move it to config json later for cross-chain compatibility)
// TODO @somnathb1 Probably not needed outside of EVM
const (
	WithdrawalRequestDataLen = 76 // addr + pubkey + amt
)

func DequeueWithdrawalRequests7002(syscall consensus.SystemCall) types.Requests {
	res, err := syscall(params.WithdrawalRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to WithdrawalRequestAddress", "err", err)
		return nil
	}
	// Just append the contract outputs
	var reqs types.Requests
	for i := 0; i <= len(res)-WithdrawalRequestDataLen; i += WithdrawalRequestDataLen {

		wr := &types.WithdrawalRequest{
			RequestData: [WithdrawalRequestDataLen]byte(res[i : i+WithdrawalRequestDataLen]),
		}
		reqs = append(reqs, wr)
	}
	return reqs
}
