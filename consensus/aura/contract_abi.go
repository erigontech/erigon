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

package aura

import (
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/aura/contracts"
)

func callBlockRewardAbi(contractAddr libcommon.Address, syscall consensus.SystemCall, beneficiaries []libcommon.Address, rewardKind []consensus.RewardKind) ([]libcommon.Address, []*uint256.Int) {
	castedKind := make([]uint16, len(rewardKind))
	for i := range rewardKind {
		castedKind[i] = uint16(rewardKind[i])
	}
	packed, err := blockRewardAbi().Pack("reward", beneficiaries, castedKind)
	if err != nil {
		panic(err)
	}
	out, err := syscall(contractAddr, packed)
	if err != nil {
		panic(err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	res, err := blockRewardAbi().Unpack("reward", out)
	if err != nil {
		panic(err)
	}
	beneficiariesRes := res[0].([]libcommon.Address)
	rewardsBig := res[1].([]*big.Int)
	rewardsU256 := make([]*uint256.Int, len(rewardsBig))
	for i := 0; i < len(rewardsBig); i++ {
		var overflow bool
		rewardsU256[i], overflow = uint256.FromBig(rewardsBig[i])
		if overflow {
			panic("Overflow in callBlockRewardAbi")
		}
	}
	return beneficiariesRes, rewardsU256
}

func callBlockGasLimitAbi(contractAddr libcommon.Address, syscall consensus.SystemCall) *uint256.Int {
	packed, err := blockGasLimitAbi().Pack("blockGasLimit")
	if err != nil {
		panic(err)
	}
	out, err := syscall(contractAddr, packed)
	if err != nil {
		panic(err)
	}
	if len(out) == 0 {
		return uint256.NewInt(0)
	}
	res, err := blockGasLimitAbi().Unpack("blockGasLimit", out)
	if err != nil {
		panic(err)
	}

	val, overflow := uint256.FromBig(res[0].(*big.Int))
	if overflow {
		panic("Overflow casting bigInt value to uint256")
	}
	return val
}

func blockGasLimitAbi() abi.ABI {
	return contracts.BlockGasLimitABI
}

func blockRewardAbi() abi.ABI {
	return contracts.BlockRewardABI
}

func certifierAbi() abi.ABI {
	return contracts.CertifierABI
}

func registrarAbi() abi.ABI {
	return contracts.RegistrarABI
}

func withdrawalAbi() abi.ABI {
	return contracts.WithdrawalABI
}

var serviceTransactionCheckerHashedKey, _ = libcommon.HashData([]byte("service_transaction_checker"))

func getCertifier(registrar libcommon.Address, syscall consensus.SystemCall) *libcommon.Address {
	packed, err := registrarAbi().Pack("getAddress", serviceTransactionCheckerHashedKey, "A")
	if err != nil {
		panic(err)
	}
	out, err := syscall(registrar, packed)
	if err != nil {
		panic(err)
	}
	if len(out) == 0 {
		return nil
	}
	res, err := registrarAbi().Unpack("getAddress", out)
	if err != nil {
		panic(err)
	}
	certifier := res[0].(libcommon.Address)
	return &certifier
}
