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

	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
)

// See https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7002.md#system-call
func DequeueWithdrawalRequests7002(syscall consensus.SystemCall, state *state.IntraBlockState) (*types.FlatRequest, error) {
	codeSize, err := state.GetCodeSize(params.WithdrawalRequestAddress)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf("[EIP-7002] Syscall failure: Empty Code at WithdrawalRequestAddress=%x", params.WithdrawalRequestAddress)
	}

	res, err := syscall(params.WithdrawalRequestAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("[EIP-7002] Unprecedented Syscall failure WithdrawalRequestAddress=%x error=%s", params.WithdrawalRequestAddress, err.Error())
	}
	if res != nil {
		// Just append the contract output
		return &types.FlatRequest{Type: types.WithdrawalRequestType, RequestData: res}, nil
	}
	return nil, nil
}
