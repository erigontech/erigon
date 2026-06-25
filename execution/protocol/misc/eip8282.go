// Copyright 2026 The Erigon Authors
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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-8282 builder deposit contract runtime bytecode.
// geas-compiled derivative of the EIP-7002 withdrawal contract — same dispatch,
// fee mechanism, queue, and storage layout with a wider record (184 bytes) and
// two additional value checks (amount >= BUILDER_MIN_DEPOSIT, msg.value >= fee + amount*1gwei).
var BuilderDepositRequestCode = common.FromHex("0x3373fffffffffffffffffffffffffffffffffffffffe146101065760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461023457600182026001905f5b5f82111560695781019083028483029004916001019190604e565b90939004925050503660b814608957366102345734610234575f5260205ff35b8034106102345760383567ffffffffffffffff1680633b9aca001161023457633b9aca00029034031061023457600154600101600155600354806006026004015f358155600101602035815560010160403581556001016060358155600101608035815560010160a035905560b85f5f3760b85fa0600101600355005b600354600254808203806101001161011d57506101005b5f5b8181146101c3578281016006026004018160b8028154815260200181600101548152602001816002015480825260401c67ffffffffffffffff16816010018160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360200181600301548152602001816004015481526020019060050154905260010161011f565b91018092146101d557906002556101e0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561020d57505f5b6001546020828201116102225750505f610228565b01602090035b5f555f60015560b8025ff35b5f5ffd")

// EIP-8282 builder exit contract runtime bytecode.
// geas-compiled derivative of the EIP-7002 withdrawal contract — same skeleton
// with the amount field removed, producing a 68-byte record (source_address ++ pubkey).
var BuilderExitRequestCode = common.FromHex("0x3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461018857600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603014608857366101885734610188575f5260205ff35b341061018857600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260305f60143760445fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101175782810160030260040181604402815460601b8152601401816001015481526020019060020154905260010160e1565b91018092146101295790600255610134565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561016157505f5b6001546002828201116101765750505f61017c565b01600290035b5f555f6001556044025ff35b5f5ffd")

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
