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

// Package aura implements the proof-of-authority consensus engine.
package aura

import (
	"errors"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
)

// Draws a validator nonce modulo number of validators.
func GetFromValidatorSet(set ValidatorSet, parent common.Hash, nonce uint, call consensus.Call) (common.Address, error) {
	//d, err := set.defaultCaller(parent)
	//if err != nil {
	//	return common.Address{}, err
	//}
	return set.getWithCaller(parent, nonce, call)
}

func newValidatorSetFromJson(j *chain.ValidatorSetJson, posdaoTransition *uint64) ValidatorSet {
	if j.List != nil {
		return &SimpleList{validators: j.List}
	}
	if j.SafeContract != nil {
		return NewValidatorSafeContract(*j.SafeContract, posdaoTransition, nil)
	}
	if j.Contract != nil {
		return &ValidatorContract{
			contractAddress:  *j.Contract,
			validators:       NewValidatorSafeContract(*j.Contract, posdaoTransition, nil),
			posdaoTransition: posdaoTransition,
		}
	}
	if j.Multi != nil {
		l := map[uint64]ValidatorSet{}
		for block, set := range j.Multi {
			l[block] = newValidatorSetFromJson(set, posdaoTransition)
		}
		return NewMulti(l)
	}

	return nil
}

type Code struct {
	Code     []byte
	CodeHash common.Hash
}

type BlockRewardContract struct {
	blockNum uint64
	address  common.Address // On-chain address.
}

type BlockRewardContractList []BlockRewardContract

func (r BlockRewardContractList) Less(i, j int) bool { return r[i].blockNum < r[j].blockNum }
func (r BlockRewardContractList) Len() int           { return len(r) }
func (r BlockRewardContractList) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type BlockReward struct {
	blockNum uint64
	amount   *uint256.Int
}

type BlockRewardList []BlockReward

func (r BlockRewardList) Less(i, j int) bool { return r[i].blockNum < r[j].blockNum }
func (r BlockRewardList) Len() int           { return len(r) }
func (r BlockRewardList) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

func NewBlockRewardContract(address common.Address) *BlockRewardContract {
	return &BlockRewardContract{address: address}
}

type AuthorityRoundParams struct {
	// A map defining intervals of blocks with the given times (in seconds) to wait before next
	// block or authority switching. The keys in the map are steps of starting blocks of those
	// periods. The entry at `0` should be defined.
	//
	// Wait times (durations) are additionally required to be less than 65535 since larger values
	// lead to slow block issuance.
	StepDurations map[uint64]uint64
	// Starting step,
	StartStep *uint64
	// Valid validators.
	Validators ValidatorSet
	// Chain score validation transition block.
	ValidateScoreTransition uint64
	// Monotonic step validation transition block.
	ValidateStepTransition uint64
	// Immediate transitions.
	ImmediateTransitions bool
	// Block reward in base units.
	BlockReward BlockRewardList
	// Block reward contract addresses with their associated starting block numbers.
	BlockRewardContractTransitions BlockRewardContractList
	// Number of accepted uncles transition block.
	MaximumUncleCountTransition uint64
	// Number of accepted uncles.
	MaximumUncleCount uint
	// Transition block to strict empty steps validation.
	StrictEmptyStepsTransition uint64
	// If set, enables random number contract integration. It maps the transition block to the contract address.
	RandomnessContractAddress map[uint64]common.Address
	// The addresses of contracts that determine the block gas limit with their associated block
	// numbers.
	BlockGasLimitContractTransitions map[uint64]common.Address
	// If set, this is the block number at which the consensus engine switches from AuRa to AuRa
	// with POSDAO modifications.
	PosdaoTransition *uint64
	// Stores human-readable keys associated with addresses, like DNS information.
	// This contract is primarily required to store the address of the Certifier contract.
	Registrar *common.Address

	// See https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
	WithdrawalContractAddress *common.Address

	RewriteBytecode map[uint64]map[common.Address][]byte
}

func FromJson(jsonParams *chain.AuRaConfig) (AuthorityRoundParams, error) {
	params := AuthorityRoundParams{
		Validators:                       newValidatorSetFromJson(jsonParams.Validators, jsonParams.PosdaoTransition),
		StartStep:                        jsonParams.StartStep,
		RandomnessContractAddress:        jsonParams.RandomnessContractAddress,
		BlockGasLimitContractTransitions: jsonParams.BlockGasLimitContractTransitions,
		PosdaoTransition:                 jsonParams.PosdaoTransition,
		Registrar:                        jsonParams.Registrar,
		WithdrawalContractAddress:        jsonParams.WithdrawalContractAddress,
	}
	params.StepDurations = map[uint64]uint64{}
	if jsonParams.StepDuration != nil {
		params.StepDurations[0] = *jsonParams.StepDuration
	}

	for blockNum, address := range jsonParams.BlockRewardContractTransitions {
		params.BlockRewardContractTransitions = append(params.BlockRewardContractTransitions, BlockRewardContract{blockNum: uint64(blockNum), address: address})
	}
	sort.Sort(params.BlockRewardContractTransitions)
	if jsonParams.BlockRewardContractAddress != nil {
		transitionBlockNum := uint64(0)
		if jsonParams.BlockRewardContractTransition != nil {
			transitionBlockNum = *jsonParams.BlockRewardContractTransition
		}
		if len(params.BlockRewardContractTransitions) > 0 && transitionBlockNum >= params.BlockRewardContractTransitions[0].blockNum {
			return params, errors.New("blockRewardContractTransition should be less than any of the keys in BlockRewardContractTransitions")
		}
		contract := BlockRewardContract{blockNum: transitionBlockNum, address: *jsonParams.BlockRewardContractAddress}
		params.BlockRewardContractTransitions = append(BlockRewardContractList{contract}, params.BlockRewardContractTransitions...)
	}

	if jsonParams.ValidateScoreTransition != nil {
		params.ValidateScoreTransition = *jsonParams.ValidateScoreTransition
	}
	if jsonParams.ValidateStepTransition != nil {
		params.ValidateStepTransition = *jsonParams.ValidateStepTransition
	}
	if jsonParams.ImmediateTransitions != nil {
		params.ImmediateTransitions = *jsonParams.ImmediateTransitions
	}
	if jsonParams.MaximumUncleCount != nil {
		params.MaximumUncleCount = *jsonParams.MaximumUncleCount
	}
	if jsonParams.MaximumUncleCountTransition != nil {
		params.MaximumUncleCountTransition = *jsonParams.MaximumUncleCountTransition
	}

	if jsonParams.BlockReward == nil {
		params.BlockReward = append(params.BlockReward, BlockReward{blockNum: 0, amount: u256.Num0})
	} else {
		if jsonParams.BlockReward != nil {
			params.BlockReward = append(params.BlockReward, BlockReward{blockNum: 0, amount: uint256.NewInt(*jsonParams.BlockReward)})
		}
	}
	sort.Sort(params.BlockReward)

	params.RewriteBytecode = make(map[uint64]map[common.Address][]byte, len(jsonParams.RewriteBytecode))
	for block, overrides := range jsonParams.RewriteBytecode {
		params.RewriteBytecode[block] = make(map[common.Address][]byte, len(overrides))
		for address, code := range overrides {
			params.RewriteBytecode[block][address] = []byte(code)
		}
	}

	return params, nil
}
