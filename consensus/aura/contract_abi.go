package aura

import (
	"bytes"
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura/contracts"
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
	a, err := abi.JSON(bytes.NewReader(contracts.BlockGasLimit))
	if err != nil {
		panic(err)
	}
	return a
}

func blockRewardAbi() abi.ABI {
	a, err := abi.JSON(bytes.NewReader(contracts.BlockReward))
	if err != nil {
		panic(err)
	}
	return a
}

func certifierAbi() abi.ABI {
	a, err := abi.JSON(bytes.NewReader(contracts.Certifier))
	if err != nil {
		panic(err)
	}
	return a
}

func registrarAbi() abi.ABI {
	a, err := abi.JSON(bytes.NewReader(contracts.Registrar))
	if err != nil {
		panic(err)
	}
	return a
}

func withdrawalAbi() abi.ABI {
	a, err := abi.JSON(bytes.NewReader(contracts.Withdrawal))
	if err != nil {
		panic(err)
	}
	return a
}

func getCertifier(registrar libcommon.Address, syscall consensus.SystemCall) *libcommon.Address {
	hashedKey, err := common.HashData([]byte("service_transaction_checker"))
	if err != nil {
		panic(err)
	}
	packed, err := registrarAbi().Pack("getAddress", hashedKey, "A")
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
