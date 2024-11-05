package bor

import (
	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
)

var transferLogSig = libcommon.HexToHash("0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4")
var transferFeeLogSig = libcommon.HexToHash("0x4dfe1bbbcf077ddc3e01291eea2d5c70c2b422b415d95645b9adcfd678cb1d63")
var feeAddress = libcommon.HexToAddress("0x0000000000000000000000000000000000001010")

// addTransferLog adds transfer log into state
func addTransferLog(
	state evmtypes.IntraBlockState,
	eventSig libcommon.Hash,

	sender,
	recipient libcommon.Address,

	amount,
	input1,
	input2,
	output1,
	output2 *uint256.Int,
) {
	// ignore if amount is 0
	if amount.IsZero() {
		return
	}

	data := make([]byte, 32*5)
	amount.WriteToSlice(data)
	input1.WriteToSlice(data[32:])
	input2.WriteToSlice(data[64:])
	output1.WriteToSlice(data[96:])
	output2.WriteToSlice(data[128:])

	// add transfer log
	state.AddLog(&types.Log{
		Address: feeAddress,
		Topics: []libcommon.Hash{
			eventSig,
			feeAddress.Hash(),
			sender.Hash(),
			recipient.Hash(),
		},
		Data: data,
	})
}
