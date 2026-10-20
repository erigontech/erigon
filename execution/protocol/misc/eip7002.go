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
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var WithdrawalRequestCode = common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd")

// See https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7002.md#system-call
func DequeueWithdrawalRequests7002(syscall rules.SystemCall, state *state.IntraBlockState, withdrawalRequestAddress accounts.Address) (*types.FlatRequest, error) {
	codeSize, err := state.GetCodeSize(withdrawalRequestAddress)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf("[EIP-7002] Syscall failure: Empty Code at WithdrawalRequestAddress=%x", withdrawalRequestAddress)
	}

	res, err := syscall(withdrawalRequestAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("[EIP-7002] Unprecedented Syscall failure WithdrawalRequestAddress=%x error=%s", withdrawalRequestAddress, err.Error())
	}
	if res != nil {
		// Just append the contract output
		return &types.FlatRequest{Type: types.WithdrawalRequestType, RequestData: slices.Clone(res)}, nil
	}
	return nil, nil
}
