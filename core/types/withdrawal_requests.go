package types

import libcommon "github.com/ledgerwatch/erigon-lib/common"

type WithdrawalRequest struct {
	SourceAddress   libcommon.Address
	ValidatorPubkey [pLen]byte // bls
	Amount          uint64
}

type WithdrawalRequests []*WithdrawalRequest
