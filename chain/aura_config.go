/*
   Copyright 2023 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package chain

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

// Different ways of specifying validators.
type ValidatorSetJson struct {
	// A simple list of authorities.
	List []common.Address `json:"list"`
	// Address of a contract that indicates the list of authorities.
	SafeContract *common.Address `json:"safeContract"`
	// Address of a contract that indicates the list of authorities and enables reporting of their misbehaviour using transactions.
	Contract *common.Address `json:"contract"`
	// A map of starting blocks for each validator set.
	Multi map[uint64]*ValidatorSetJson `json:"multi"`
}

// AuRaConfig is the consensus engine configs for proof-of-authority based sealing.
type AuRaConfig struct {
	StepDuration *uint64           `json:"stepDuration"` // Block duration, in seconds.
	Validators   *ValidatorSetJson `json:"validators"`   // Valid authorities

	// Starting step. Determined automatically if not specified.
	// To be used for testing only.
	StartStep               *uint64 `json:"startStep"`
	ValidateScoreTransition *uint64 `json:"validateScoreTransition"` // Block at which score validation should start.
	ValidateStepTransition  *uint64 `json:"validateStepTransition"`  // Block from which monotonic steps start.
	ImmediateTransitions    *bool   `json:"immediateTransitions"`    // Whether transitions should be immediate.
	BlockReward             *uint64 `json:"blockReward"`             // Reward per block in wei.
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
	// used simultaneously in the same configuration. In such a case the code requires that the
	// block number of the single transition is strictly less than any of the block numbers in the
	// map.
	BlockRewardContractTransitions map[uint]common.Address `json:"blockRewardContractTransitions"`
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
	// Stores human-readable keys associated with addresses, like DNS information.
	// This contract is primarily required to store the address of the Certifier contract.
	Registrar *common.Address `json:"registrar"`

	// See https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
	WithdrawalContractAddress *common.Address `json:"withdrawalContractAddress"`

	RewriteBytecode map[uint64]map[common.Address]hexutility.Bytes `json:"rewriteBytecode"`
}

// String implements the stringer interface, returning the consensus engine details.
func (c *AuRaConfig) String() string {
	return "aura"
}
