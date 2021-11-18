// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package clique implements the proof-of-authority consensus engine.
package aura

import (
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
)

// Draws an validator nonce modulo number of validators.
func GetFromValidatorSet(set ValidatorSet, parent common.Hash, nonce uint, call consensus.Call) (common.Address, error) {
	//d, err := set.defaultCaller(parent)
	//if err != nil {
	//	return common.Address{}, err
	//}
	return set.getWithCaller(parent, nonce, call)
}

// Different ways of specifying validators.
type ValidatorSetJson struct {
	// A simple list of authorities.
	List []common.Address `json:"list"`
	// Address of a contract that indicates the list of authorities.
	SafeContract *common.Address `json:"safeContract"`
	// Address of a contract that indicates the list of authorities and enables reporting of theor misbehaviour using transactions.
	Contract *common.Address `json:"contract"`
	// A map of starting blocks for each validator set.
	Multi map[uint64]*ValidatorSetJson `json:"multi"`
}

func newValidatorSetFromJson(j *ValidatorSetJson, posdaoTransition *uint64) ValidatorSet {
	if j.List != nil {
		return &SimpleList{validators: j.List}
	}
	if j.SafeContract != nil {
		return NewValidatorSafeContract(*j.SafeContract, posdaoTransition, nil)
	}
	if j.Contract != nil {
		return &ValidatorContract{
			contractAddress:  *j.SafeContract,
			validators:       ValidatorSafeContract{contractAddress: *j.SafeContract, posdaoTransition: posdaoTransition},
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

//TODO: StepDuration and BlockReward - now are uint64, but it can be an object in non-sokol consensus
type JsonSpec struct {
	StepDuration *uint64           `json:"stepDuration"` // Block duration, in seconds.
	Validators   *ValidatorSetJson `json:"validators"`   // Valid authorities

	// Starting step. Determined automatically if not specified.
	// To be used for testing only.
	StartStep               *uint64  `json:"startStep"`
	ValidateScoreTransition *uint64  `json:"validateScoreTransition"` // Block at which score validation should start.
	ValidateStepTransition  *uint64  `json:"validateStepTransition"`  // Block from which monotonic steps start.
	ImmediateTransitions    *bool    `json:"immediateTransitions"`    // Whether transitions should be immediate.
	BlockReward             *big.Int `json:"blockReward"`             // Reward per block in wei.
	// Block at which the block reward contract should start being used. This option allows one to
	// add a single block reward contract transition and is compatible with the multiple address
	// option `block_reward_contract_transitions` below.
	BlockRewardContractTransition *uint64 `json:"blockRewardContractTransition"`
	/// Block reward contract address which overrides the `block_reward` setting. This option allows
	/// one to add a single block reward contract address and is compatible with the multiple
	/// address option `block_reward_contract_transitions` below.
	BlockRewardContractAddress *common.Address `json:"blockRewardContractAddress"`
	// Block reward contract addresses with their associated starting block numbers.
	//
	// Setting the block reward contract overrides `block_reward`. If the single block reward
	// contract address is also present then it is added into the map at the block number stored in
	// `block_reward_contract_transition` or 0 if that block number is not provided. Therefore both
	// a single block reward contract transition and a map of reward contract transitions can be
	// used simulataneously in the same configuration. In such a case the code requires that the
	// block number of the single transition is strictly less than any of the block numbers in the
	// map.
	BlockRewardContractTransitions map[uint]common.Address `json:"blockRewardContractTransitions"`
	/// Block reward code. This overrides the block reward contract address.
	BlockRewardContractCode []byte `json:"blockRewardContractCode"`
	// Block at which maximum uncle count should be considered.
	MaximumUncleCountTransition *uint64 `json:"maximumUncleCountTransition"`
	// Maximum number of accepted uncles.
	MaximumUncleCount *uint `json:"maximumUncleCount"`
	// Strict validation of empty steps transition block.
	StrictEmptyStepsTransition *uint `json:"strictEmptyStepsTransition"`
	// The random number contract's address, or a map of contract transitions.
	RandomnessContractAddress map[uint64]common.Address `json:"randomnessContractAddress"`
	// The addresses of contracts that determine the block gas limit starting from the block number
	// associated with each of those contracts.
	BlockGasLimitContractTransitions map[uint64]common.Address `json:"blockGasLimitContractTransitions"`
	// The block number at which the consensus engine switches from AuRa to AuRa with POSDAO
	// modifications.
	PosdaoTransition *uint64 `json:"PosdaoTransition"`
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
	// time (in seconds) to wait before next block or authority switching.
	StepDuration uint64
	// Chain score validation transition block.
	ValidateScoreTransition uint64
	// Monotonic step validation transition block.
	ValidateStepTransition uint64
	// Block reward in base units.
	BlockReward big.Int
	// SafeContractTransition is when we switch to the safeContract (basic consensus contract)
	SafeContractTransition map[uint64]common.Address
	// ValidatorSetChange is the change in validators before the consensus contract come into action
	ValidatorSetChange map[uint64][]common.Address
	// If set, enables random number contract integration. It maps the transition block to the contract address.

	// The addresses of contracts that determine the block gas limit with their associated block
	// numbers.

	// Other fields that we can deal with later on, for now the objective is to run Kovan

	// MaximumUncleCount uint
	// MaximumUncleCountTransition uint64
	// StrictEmptyStepsTransition uint64
	// RandomnessContractAddress map[uint64]common.Address
	// BlockGasLimitContractTransitions map[uint64]common.Address
	// BlockRewardContractTransitions BlockRewardContractList

	// If set, this is the block number at which the consensus engine switches from AuRa to AuRa
	// with POSDAO modifications.
	PosdaoTransition uint64
	isPosdao         bool
}

func FromJson(jsonParams JsonSpec) (AuthorityRoundParams, error) {
	params := AuthorityRoundParams{
		ValidateScoreTransition: *jsonParams.ValidateScoreTransition,
		ValidateStepTransition:  *jsonParams.ValidateStepTransition,
		StepDuration:            *jsonParams.StepDuration,
		BlockReward:             *jsonParams.BlockReward,
	}
	if jsonParams.PosdaoTransition != nil {
		params.PosdaoTransition = *jsonParams.PosdaoTransition
		params.isPosdao = true
	}

	for block_number, validatorSet := range jsonParams.Validators.Multi {
		if validatorSet.SafeContract != nil {
			params.SafeContractTransition[block_number] = *validatorSet.SafeContract
			continue
		}
		params.ValidatorSetChange[block_number] = validatorSet.List
	}

	return params, nil
}
