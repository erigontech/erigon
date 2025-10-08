package cltypes

import "github.com/erigontech/erigon/common"

type BuilderPendingWithdrawal struct {
	FeeRecipient      common.Address `json:"fee_recipient"`
	Amount            uint64         `json:"amount"`
	BuilderIndex      uint64         `json:"builder_index"`
	WithdrawableEpoch uint64         `json:"withdrawable_epoch"`
}

type BuilderPendingPayment struct {
	Weight     uint64                    `json:"weight"`
	Withdrawal *BuilderPendingWithdrawal `json:"withdrawal"`
}
