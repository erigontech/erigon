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

var ConsolidationRequestCode = common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd")

// See https://github.com/ethereum/EIPs/blob/master/EIPS/eip-7251.md#system-call
func DequeueConsolidationRequests7251(syscall rules.SystemCall, state *state.IntraBlockState, consolidationRequestAddress accounts.Address) (*types.FlatRequest, error) {
	codeSize, err := state.GetCodeSize(consolidationRequestAddress)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf("[EIP-7251] Syscall failure: Empty Code at ConsolidationRequestAddress=%x", consolidationRequestAddress)
	}

	res, err := syscall(consolidationRequestAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("[EIP-7251] Unprecedented Syscall failure: ConsolidationRequestAddress=%x error=%s", consolidationRequestAddress, err.Error())
	}
	if res != nil {
		// Just append the contract output as the request data
		return &types.FlatRequest{Type: types.ConsolidationRequestType, RequestData: slices.Clone(res)}, nil
	}
	return nil, nil
}
