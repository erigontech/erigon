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
var BuilderDepositRequestCode = common.FromHex(
	"0x3373fffffffffffffffffffffffffffffffffffffffe1461011c575f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146102705760015460088111605257506058565b60089003015b601190600182026001905f5b5f821115607f57810190830284830290049160010191906064565b90939004925050503660b814609f57366102705734610270575f5260205ff35b8034106102705760383567ffffffffffffffff1680633b9aca001161027057633b9aca00029034031061027057600154600101600155600354806006026004015f358155600101602035815560010160403581556001016060358155600101608035815560010160a035905560b85f5f3760b85fa0600101600355005b60035460025480820380604011610131575060405b5f5b8181146101d7578281016006026004018160b8028154815260200181600101548152602001816002015480825260401c67ffffffffffffffff16816010018160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c816001015353602001816003015481526020018160040154815260200190600501549052600101610133565b91018092146101e957906002556101f4565b90505f6002555f6003555b36610242575f54600154817fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461023057600882820111610238575b50505f610264565b0160089003610264565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff5b5f555f60015560b8025ff35b5f5ffd",
)

// EIP-8282 builder exit contract runtime bytecode.
// geas-compiled derivative of the EIP-7002 withdrawal contract — same skeleton
// with the amount field removed, producing a 68-byte record (source_address ++ pubkey).
var BuilderExitRequestCode = common.FromHex(
	"0x3373fffffffffffffffffffffffffffffffffffffffe1460e1575f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101c65760015460028111605157506057565b60029003015b601190600182026001905f5b5f821115607e57810190830284830290049160010191906063565b909390049250505036603014609e57366101c657346101c6575f5260205ff35b34106101c657600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260305f60143760445fa0600101600355005b6003546002548082038060101160f5575060105b5f5b81811461012d5782810160030260040181604402815460601b8152601401816001015481526020019060020154905260010160f7565b910180921461013f579060025561014a565b90505f6002555f6003555b36610198575f54600154817fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101865760028282011161018e575b50505f6101ba565b01600290036101ba565b7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff5b5f555f6001556044025ff35b5f5ffd",
)

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
