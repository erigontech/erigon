// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package misc

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/abi"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"
)

const (
	BLSPubKeyLen             = 48
	WithdrawalCredentialsLen = 32 // withdrawalCredentials size
	BLSSigLen                = 96 // signature size
	DepositLogLen            = 576
)

var depositTopic = libcommon.HexToHash("0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5")

var (
	// DepositABI is an ABI instance of beacon chain deposit events.
	DepositABI   = abi.ABI{Events: map[string]abi.Event{"DepositEvent": depositEvent}}
	bytesT, _    = abi.NewType("bytes", "", nil)
	depositEvent = abi.NewEvent("DepositEvent", "DepositEvent", false, abi.Arguments{
		{Name: "pubkey", Type: bytesT, Indexed: false},
		{Name: "withdrawal_credentials", Type: bytesT, Indexed: false},
		{Name: "amount", Type: bytesT, Indexed: false},
		{Name: "signature", Type: bytesT, Indexed: false},
		{Name: "index", Type: bytesT, Indexed: false}},
	)
)

// field type overrides for abi upacking
type depositUnpacking struct {
	Pubkey                []byte
	WithdrawalCredentials []byte
	Amount                []byte
	Signature             []byte
	Index                 []byte
}

var InvalidDepositLogErr = errors.New("invalid deposit log: unsupported data layout")

func validateDepositLog(data []byte) error {
	if len(data) != DepositLogLen {
		return InvalidDepositLogErr
	}
	pubkey_offset := uint256.NewInt(0).SetBytes(data[0:32]).Uint64()
	withdrawal_credentials_offset := uint256.NewInt(0).SetBytes(data[32:64]).Uint64()
	amount_offset := uint256.NewInt(0).SetBytes(data[64:96]).Uint64()
	signature_offset := uint256.NewInt(0).SetBytes(data[96:128]).Uint64()
	index_offset := uint256.NewInt(0).SetBytes(data[128:160]).Uint64()

	if pubkey_offset != 160 ||
		withdrawal_credentials_offset != 256 ||
		amount_offset != 320 ||
		signature_offset != 384 ||
		index_offset != 512 {
		return InvalidDepositLogErr
	}

	pubkey_size := uint256.NewInt(0).SetBytes(data[pubkey_offset : pubkey_offset+32]).Uint64()
	withdrawal_credentials_size := uint256.NewInt(0).SetBytes(data[withdrawal_credentials_offset : withdrawal_credentials_offset+32]).Uint64()
	amount_size := uint256.NewInt(0).SetBytes(data[amount_offset : amount_offset+32]).Uint64()
	signature_size := uint256.NewInt(0).SetBytes(data[signature_offset : signature_offset+32]).Uint64()
	index_size := uint256.NewInt(0).SetBytes(data[index_offset : index_offset+32]).Uint64()

	if pubkey_size != 48 ||
		withdrawal_credentials_size != 32 ||
		amount_size != 8 ||
		signature_size != 96 ||
		index_size != 8 {
		return InvalidDepositLogErr
	}
	return nil
}

// unpackDepositLog unpacks a serialized DepositEvent.
func unpackDepositLog(data []byte) ([]byte, error) {
	var du depositUnpacking
	if err := validateDepositLog(data); err != nil {
		return nil, err
	}
	if err := DepositABI.UnpackIntoInterface(&du, "DepositEvent", data); err != nil {
		return nil, err
	}
	reqData := make([]byte, 0, types.DepositRequestDataLen)
	reqData = append(reqData, du.Pubkey...)
	reqData = append(reqData, du.WithdrawalCredentials...)
	reqData = append(reqData, du.Amount...)
	reqData = append(reqData, du.Signature...)
	reqData = append(reqData, du.Index...)

	return reqData, nil
}

// ParseDepositLogs extracts the EIP-6110 deposit values from logs emitted by
// BeaconDepositContract and returns a FlatRequest object ptr
func ParseDepositLogs(logs []*types.Log, depositContractAddress libcommon.Address) (*types.FlatRequest, error) {
	if depositContractAddress == (libcommon.Address{}) {
		log.Warn("Error in ParseDepositLogs - depositContractAddress is 0x0")
	}
	reqData := make([]byte, 0, len(logs)*types.DepositRequestDataLen)
	for _, l := range logs {
		if l.Address == depositContractAddress && len(l.Topics) > 0 && l.Topics[0] == depositTopic {
			d, err := unpackDepositLog(l.Data)
			if err != nil {
				return nil, fmt.Errorf("unable to parse deposit data: %v", err)
			}
			reqData = append(reqData, d...)
		}
	}
	if len(reqData) > 0 {
		return &types.FlatRequest{Type: types.DepositRequestType, RequestData: reqData}, nil
	}
	return nil, nil
}
