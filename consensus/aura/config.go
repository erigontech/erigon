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

// Package aura implements the proof-of-authority consensus engine.
package aura

import (
	"errors"
	"sort"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
)

// Draws an validator nonce modulo number of validators.
func GetFromValidatorSet(set ValidatorSet, parent libcommon.Hash, nonce uint, call consensus.Call) (libcommon.Address, error) {
	//d, err := set.defaultCaller(parent)
	//if err != nil {
	//	return libcommon.Address{}, err
	//}
	return set.getWithCaller(parent, nonce, call)
}

// Different ways of specifying validators.
type ValidatorSetJson struct {
	// A simple list of authorities.
	List []libcommon.Address `json:"list"`
	// Address of a contract that indicates the list of authorities.
	SafeContract *libcommon.Address `json:"safeContract"`
	// Address of a contract that indicates the list of authorities and enables reporting of their misbehaviour using transactions.
	Contract *libcommon.Address `json:"contract"`
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

// TODO: StepDuration and BlockReward - now are uint64, but it can be an object in non-sokol consensus
type JsonSpec struct {
	StepDuration *uint64           `json:"stepDuration"` // Block duration, in seconds.
	Validators   *ValidatorSetJson `json:"validators"`   // Valid authorities

	// Starting step. Determined automatically if not specified.
	// To be used for testing only.
	StartStep               *uint64         `json:"startStep"`
	ValidateScoreTransition *uint64         `json:"validateScoreTransition"` // Block at which score validation should start.
	ValidateStepTransition  *uint64         `json:"validateStepTransition"`  // Block from which monotonic steps start.
	ImmediateTransitions    *bool           `json:"immediateTransitions"`    // Whether transitions should be immediate.
	BlockReward             *hexutil.Uint64 `json:"blockReward"`             // Reward per block in wei.
	// Block at which the block reward contract should start being used. This option allows one to
	// add a single block reward contract transition and is compatible with the multiple address
	// option `block_reward_contract_transitions` below.
	BlockRewardContractTransition *uint64 `json:"blockRewardContractTransition"`
	/// Block reward contract address which overrides the `block_reward` setting. This option allows
	/// one to add a single block reward contract address and is compatible with the multiple
	/// address option `block_reward_contract_transitions` below.
	BlockRewardContractAddress *libcommon.Address `json:"blockRewardContractAddress"`
	// Block reward contract addresses with their associated starting block numbers.
	//
	// Setting the block reward contract overrides `block_reward`. If the single block reward
	// contract address is also present then it is added into the map at the block number stored in
	// `block_reward_contract_transition` or 0 if that block number is not provided. Therefore both
	// a single block reward contract transition and a map of reward contract transitions can be
	// used simultaneously in the same configuration. In such a case the code requires that the
	// block number of the single transition is strictly less than any of the block numbers in the
	// map.
	BlockRewardContractTransitions map[uint]libcommon.Address `json:"blockRewardContractTransitions"`
	// Block at which maximum uncle count should be considered.
	MaximumUncleCountTransition *uint64 `json:"maximumUncleCountTransition"`
	// Maximum number of accepted uncles.
	MaximumUncleCount *uint `json:"maximumUncleCount"`
	// Strict validation of empty steps transition block.
	StrictEmptyStepsTransition *uint `json:"strictEmptyStepsTransition"`
	// The random number contract's address, or a map of contract transitions.
	RandomnessContractAddress map[uint64]libcommon.Address `json:"randomnessContractAddress"`
	// The addresses of contracts that determine the block gas limit starting from the block number
	// associated with each of those contracts.
	BlockGasLimitContractTransitions map[uint64]libcommon.Address `json:"blockGasLimitContractTransitions"`
	// The block number at which the consensus engine switches from AuRa to AuRa with POSDAO
	// modifications.
	PosdaoTransition *uint64 `json:"PosdaoTransition"`
	// Stores human-readable keys associated with addresses, like DNS information.
	// This contract is primarily required to store the address of the Certifier contract.
	Registrar *libcommon.Address `json:"registrar"`

	// See https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
	WithdrawalContractAddress *libcommon.Address `json:"withdrawalContractAddress"`

	RewriteBytecode map[uint64]map[libcommon.Address]hexutil.Bytes `json:"rewriteBytecode"`
}

type Code struct {
	Code     []byte
	CodeHash libcommon.Hash
}

type BlockRewardContract struct {
	blockNum uint64
	address  libcommon.Address // On-chain address.
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

func NewBlockRewardContract(address libcommon.Address) *BlockRewardContract {
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
	RandomnessContractAddress map[uint64]libcommon.Address
	// The addresses of contracts that determine the block gas limit with their associated block
	// numbers.
	BlockGasLimitContractTransitions map[uint64]libcommon.Address
	// If set, this is the block number at which the consensus engine switches from AuRa to AuRa
	// with POSDAO modifications.
	PosdaoTransition *uint64
	// Stores human-readable keys associated with addresses, like DNS information.
	// This contract is primarily required to store the address of the Certifier contract.
	Registrar *libcommon.Address

	// See https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
	WithdrawalContractAddress *libcommon.Address

	RewriteBytecode map[uint64]map[libcommon.Address][]byte
}

func FromJson(jsonParams JsonSpec) (AuthorityRoundParams, error) {
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
			params.BlockReward = append(params.BlockReward, BlockReward{blockNum: 0, amount: uint256.NewInt(uint64(*jsonParams.BlockReward))})
		}
	}
	sort.Sort(params.BlockReward)

	params.RewriteBytecode = make(map[uint64]map[libcommon.Address][]byte, len(jsonParams.RewriteBytecode))
	for block, overrides := range jsonParams.RewriteBytecode {
		params.RewriteBytecode[block] = make(map[libcommon.Address][]byte, len(overrides))
		for address, code := range overrides {
			params.RewriteBytecode[block][address] = []byte(code)
		}
	}

	return params, nil
}
