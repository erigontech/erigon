package misc

import (
	"encoding/binary"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

// Configuration related to EIP-7002
// (May have to move it to config json later for cross-chain compatibility)
// TODO @somnathb1 Probably not needed outside of EVM
const (
	ExcessWithdrawalReqsSlot        = 0
	WithdrawalReqCountSlot          = 1
	WithdrawalReqQueueHeadSlot      = 2
	WithdrawalReqQueueTailSlot      = 3
	WithdrawalReqQueueStorageOffset = 4
	MaxWithdrawalReqsPerBlock       = 16
	TargetWithdrawalReqsPerBlock    = 2
	MinWithdrawalReqFee             = 1
	WithdrawalReqFeeUpdFraction     = 17
)

// const abiStr = `[
// 	{
// 		"inputs": [],
// 		"name": "read_withdrawal_requests",
// 		"outputs": [
// 			{
// 				"components": [
// 					{
// 						"internalType": "bytes20",
// 						"name": "sourceAddress",
// 						"type": "bytes20"
// 					},
// 					{
// 						"internalType": "bytes32",
// 						"name": "validatorPubKey1",
// 						"type": "bytes32"
// 					},
// 					{
// 						"internalType": "bytes16",
// 						"name": "validatorPubKey2",
// 						"type": "bytes16"
// 					},
// 					{
// 						"internalType": "uint64",
// 						"name": "amount",
// 						"type": "uint64"
// 					}
// 				],
// 				"internalType": "struct WithdrawalContract.ValidatorWithdrawalRequest[]",
// 				"name": "",
// 				"type": "tuple[]"
// 			}
// 		],
// 		"stateMutability": "nonpayable",
// 		"type": "function"
// 	}
// ]`

func DequeueWithdrawalRequests7002(syscall consensus.SystemCall) types.Requests {
	res, err := syscall(params.WithdrawalRequestAddress, nil)
	if err != nil {
		log.Warn("Err with syscall to WithdrawalRequestAddress", "err", err)
		return nil
	}
	// Parse out the exits - using the bytes array returned
	var reqs types.Requests
	lenPerReq := 20 + 48 + 8 // addr + pubkey + amt
	for i := 0; i <= len(res)-lenPerReq; i += lenPerReq {
		var pubkey [48]byte
		copy(pubkey[:], res[i+20:i+68])
		wr := &types.WithdrawalRequest{
			SourceAddress:   common.BytesToAddress(res[i : i+20]),
			ValidatorPubkey: pubkey,
			Amount:          binary.BigEndian.Uint64(res[i+68:]),
		}
		reqs = append(reqs, wr)
	}
	return reqs

	// Alternatively unpack using the abi methods
	// wAbi, _ := abi.JSON(strings.NewReader(abiStr))
	// wAbi.Unpack("read_withdrawal_requests", wrs)

	// type R struct {
	// 	sourceAddress [20]byte
	// 	validatorPubKey1 [32] byte
	// 	validatorPubKey2 [16] byte
	// 	amount uint64
	// }
	// Ret := make([]R, 0)
	// wAbi.UnpackIntoInterface(Ret, "read_withdrawal_requests", wrs)

	// reqs := make(types.Requests, 0)

	// for r := range(Ret) {
	// 	req := types.NewRequest(Ret)
	// 	reqs = append(reqs, types.NewRequest())
	// }
}
