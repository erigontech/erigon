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
	"fmt"

	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TODO(EIP-8282): Define final compiled contract bytecodes here when finalized:
var BuilderDepositRequestCode = []byte{}
var BuilderExitRequestCode = []byte{}


// https://eips.ethereum.org/EIPS/eip-8282
func DequeueBuilderDepositRequests(syscall rules.SystemCall, state *state.IntraBlockState, builderDepositAddress accounts.Address) (*types.FlatRequest, error) {
	codeSize, err := state.GetCodeSize(builderDepositAddress)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf("[EIP-8282] Syscall failure: Empty Code at BuilderDepositAddress=%x", builderDepositAddress)
	}

	res, err := syscall(builderDepositAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("[EIP-8282] Unprecedented Syscall failure: BuilderDepositAddress=%x error=%s", builderDepositAddress, err.Error())
	}
	if res != nil {
		// Just append the contract output as the request data
		return &types.FlatRequest{Type: types.BuilderDepositRequestType, RequestData: res}, nil
	}
	return nil, nil
}

// https://eips.ethereum.org/EIPS/eip-8282
func DequeueBuilderExitRequests(syscall rules.SystemCall, state *state.IntraBlockState, builderExitAddress accounts.Address) (*types.FlatRequest, error) {
	codeSize, err := state.GetCodeSize(builderExitAddress)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf("[EIP-8282] Syscall failure: Empty Code at BuilderExitAddress=%x", builderExitAddress)
	}

	res, err := syscall(builderExitAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("[EIP-8282] Unprecedented Syscall failure: BuilderExitAddress=%x error=%s", builderExitAddress, err.Error())
	}
	if res != nil {
		// Just append the contract output as the request data
		return &types.FlatRequest{Type: types.BuilderExitRequestType, RequestData: res}, nil
	}
	return nil, nil
}
