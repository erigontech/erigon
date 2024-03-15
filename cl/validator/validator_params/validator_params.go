package validator_params

import (
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type ValidatorParams struct {
	feeRecipients sync.Map
}

func NewValidatorParams() *ValidatorParams {
	return &ValidatorParams{}
}

func (vp *ValidatorParams) SetFeeRecipient(validatorIndex uint64, feeRecipient libcommon.Address) {
	vp.feeRecipients.Store(validatorIndex, feeRecipient)
}

func (vp *ValidatorParams) GetFeeRecipient(validatorIndex uint64) (libcommon.Address, bool) {
	val, ok := vp.feeRecipients.Load(validatorIndex)
	if !ok {
		return libcommon.Address{}, false
	}
	return val.(libcommon.Address), true
}
