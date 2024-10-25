package cltypes

import "github.com/erigontech/erigon/cl/cltypes/solid"

// class ExecutionRequests(Container):
//
//	deposits: List[DepositRequest, MAX_DEPOSIT_REQUESTS_PER_PAYLOAD]  # [New in Electra:EIP6110]
//	withdrawals: List[WithdrawalRequest, MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD]  # [New in Electra:EIP7002:EIP7251]
//	consolidations: List[ConsolidationRequest, MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD]  # [New in Electra:EIP7251]
type ExecutionRequests struct {
	Deposits       *solid.ListSSZ[*solid.DepositRequest]       `json:"deposits"`
	Withdrawals    *solid.ListSSZ[*solid.WithdrawalRequest]    `json:"withdrawals"`
	Consolidations *solid.ListSSZ[*solid.ConsolidationRequest] `json:"consolidations"`
}
