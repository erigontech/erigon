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
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
)

type StepDuration struct {
	Single      uint          /// Duration of all steps.
	Transitions map[uint]uint /// Step duration transitions: a mapping of timestamp to step durations.
}

/// Draws an validator nonce modulo number of validators.
func GetFromValidatorSet(set ValidatorSet, parent common.Hash, nonce uint) common.Address {
	d := set.defaultCaller(parent)
	return set.getWithCaller(parent, nonce, d)
}

type BlockReward struct {
	Single uint
	Multi  map[uint]uint
}

/// Different ways of specifying validators.
type ValidatorSetJson struct {
	/// A simple list of authorities.
	List []common.Address `json:"list"`
	/// Address of a contract that indicates the list of authorities.
	SafeContract *common.Address `json:"safeContract"`
	/// Address of a contract that indicates the list of authorities and enables reporting of theor misbehaviour using transactions.
	Contract *common.Address `json:"contract"`
	/// A map of starting blocks for each validator set.
	Multi map[uint64]*ValidatorSetJson `json:"multi"`
}

type JsonSpec struct {
	StepDuration StepDuration      /// Block duration, in seconds.
	Validators   *ValidatorSetJson /// Valid authorities

	/// Starting step. Determined automatically if not specified.
	/// To be used for testing only.
	StartStep               *uint
	ValidateScoreTransition *uint        /// Block at which score validation should start.
	ValidateStepTransition  *uint        /// Block from which monotonic steps start.
	ImmediateTransitions    *bool        /// Whether transitions should be immediate.
	BlockReward             *BlockReward /// Reward per block in wei.
	/// Block at which the block reward contract should start being used. This option allows one to
	/// add a single block reward contract transition and is compatible with the multiple address
	/// option `block_reward_contract_transitions` below.
	BlockRewardContractTransition uint
	/// Block reward contract address which overrides the `block_reward` setting. This option allows
	/// one to add a single block reward contract address and is compatible with the multiple
	/// address option `block_reward_contract_transitions` below.
	BlockRewardContractAddress common.Address
	/// Block reward contract addresses with their associated starting block numbers.
	///
	/// Setting the block reward contract overrides `block_reward`. If the single block reward
	/// contract address is also present then it is added into the map at the block number stored in
	/// `block_reward_contract_transition` or 0 if that block number is not provided. Therefore both
	/// a single block reward contract transition and a map of reward contract transitions can be
	/// used simulataneously in the same configuration. In such a case the code requires that the
	/// block number of the single transition is strictly less than any of the block numbers in the
	/// map.
	BlockRewardContractTransitions map[uint]common.Address
	/// Block reward code. This overrides the block reward contract address.
	BlockRewardContractCode []byte
	/// Block at which maximum uncle count should be considered.
	MaximumUncleCountTransition *uint
	/// Maximum number of accepted uncles.
	MaximumUncleCount uint
	/// Block at which empty step messages should start.
	EmptyStepsTransition *uint
	/// Maximum number of accepted empty steps.
	MaximumEmptySteps *uint
	/// Strict validation of empty steps transition block.
	StrictEmptyStepsTransition *uint
	/// First block for which a 2/3 quorum (instead of 1/2) is required.
	TwoThirdsMajorityTransition *uint
	/// The random number contract's address, or a map of contract transitions.
	RandomnessContractAddress map[uint]common.Address
	/// The addresses of contracts that determine the block gas limit starting from the block number
	/// associated with each of those contracts.
	BlockGasLimitContractTransitions map[uint]common.Address
	/// The block number at which the consensus engine switches from AuRa to AuRa with POSDAO
	/// modifications.
	PosdaoTransition *uint
}

type Code struct {
	Code     []byte
	CodeHash common.Hash
}

/// Kind of SystemOrCodeCall, this is either an on-chain address, or code.
type SystemOrCodeCallKind struct {
	Address common.Address /// On-chain address.
	Code    *Code          /// Hard-coded code.
}

type BlockRewardContract struct {
	Kind SystemOrCodeCallKind
}

type AuthorityRoundParams struct {
	/// A map defining intervals of blocks with the given times (in seconds) to wait before next
	/// block or authority switching. The keys in the map are steps of starting blocks of those
	/// periods. The entry at `0` should be defined.
	///
	/// Wait times (durations) are additionally required to be less than 65535 since larger values
	/// lead to slow block issuance.
	StepDurations map[uint64]uint64
	/// Starting step,
	StartStep *uint64
	/// Valid validators.
	Validators ValidatorSet1
	/// Chain score validation transition block.
	ValidateScoreTransition uint64
	/// Monotonic step validation transition block.
	ValidateStepTransition uint64
	/// Immediate transitions.
	ImmediateTransitions bool
	/// Block reward in base units.
	BlockReward map[uint64]uint256.Int
	/// Block reward contract addresses with their associated starting block numbers.
	BlockRewardContractTransitions map[uint64]BlockRewardContract
	/// Number of accepted uncles transition block.
	MaximumUncleCountTransition uint64
	/// Number of accepted uncles.
	MaximumUncleCount uint
	/// Empty step messages transition block.
	EmptyStepsTransition uint64
	/// First block for which a 2/3 quorum (instead of 1/2) is required.
	TwoThirdsMajorityTransition uint64
	/// Number of accepted empty steps.
	MaximumEmptySteps uint
	/// Transition block to strict empty steps validation.
	StrictEmptyStepsTransition uint64
	/// If set, enables random number contract integration. It maps the transition block to the contract address.
	RandomnessContractAddress map[uint64]common.Address
	/// The addresses of contracts that determine the block gas limit with their associated block
	/// numbers.
	BlockGasLimitContractTransitions map[uint64]common.Address
	/// If set, this is the block number at which the consensus engine switches from AuRa to AuRa
	/// with POSDAO modifications.
	PosdaoTransition *uint64
}

func FromJson(roundParams JsonSpec) AuthorityRoundParams {
	/*
	   impl From<ethjson::spec::AuthorityRoundParams> for AuthorityRoundParams {
	       fn from(p: ethjson::spec::AuthorityRoundParams) -> Self {
	           let map_step_duration = |u: ethjson::uint::Uint| {
	               let mut step_duration_usize: usize = u.into();
	               if step_duration_usize == 0 {
	                   panic!("AuthorityRoundParams: step duration cannot be 0");
	               }w
	               if step_duration_usize > U16_MAX {
	                   warn!(target: "engine", "step duration is too high ({}), setting it to {}", step_duration_usize, U16_MAX);
	                   step_duration_usize = U16_MAX;
	               }
	               step_duration_usize as u64
	           };
	           let step_durations: BTreeMap<_, _> = match p.step_duration {
	               ethjson::spec::StepDuration::Single(u) => {
	                   iter::once((0, map_step_duration(u))).collect()
	               }
	               ethjson::spec::StepDuration::Transitions(tr) => {
	                   if tr.is_empty() {
	                       panic!("AuthorityRoundParams: step duration transitions cannot be empty");
	                   }
	                   tr.into_iter()
	                       .map(|(timestamp, u)| (timestamp.into(), map_step_duration(u)))
	                       .collect()
	               }
	           };
	           let transition_block_num = p.block_reward_contract_transition.map_or(0, Into::into);
	           let mut br_transitions: BTreeMap<_, _> = p
	               .block_reward_contract_transitions
	               .unwrap_or_default()
	               .into_iter()
	               .map(|(block_num, address)| {
	                   (
	                       block_num.into(),
	                       BlockRewardContract::new_from_address(address.into()),
	                   )
	               })
	               .collect();
	           if (p.block_reward_contract_code.is_some() || p.block_reward_contract_address.is_some())
	               && br_transitions
	                   .keys()
	                   .next()
	                   .map_or(false, |&block_num| block_num <= transition_block_num)
	           {
	               let s = "blockRewardContractTransition";
	               panic!("{} should be less than any of the keys in {}s", s, s);
	           }
	           if let Some(code) = p.block_reward_contract_code {
	               br_transitions.insert(
	                   transition_block_num,
	                   BlockRewardContract::new_from_code(Arc::new(code.into())),
	               );
	           } else if let Some(address) = p.block_reward_contract_address {
	               br_transitions.insert(
	                   transition_block_num,
	                   BlockRewardContract::new_from_address(address.into()),
	               );
	           }
	           let randomness_contract_address =
	               p.randomness_contract_address
	                   .map_or_else(BTreeMap::new, |transitions| {
	                       transitions
	                           .into_iter()
	                           .map(|(ethjson::uint::Uint(block), addr)| (block.as_u64(), addr.into()))
	                           .collect()
	                   });
	           let block_gas_limit_contract_transitions: BTreeMap<_, _> = p
	               .block_gas_limit_contract_transitions
	               .unwrap_or_default()
	               .into_iter()
	               .map(|(block_num, address)| (block_num.into(), address.into()))
	               .collect();
	           AuthorityRoundParams {
	               step_durations,
	               validators: new_validator_set_posdao(p.validators, p.posdao_transition.map(Into::into)),
	               start_step: p.start_step.map(Into::into),
	               validate_score_transition: p.validate_score_transition.map_or(0, Into::into),
	               validate_step_transition: p.validate_step_transition.map_or(0, Into::into),
	               immediate_transitions: p.immediate_transitions.unwrap_or(false),
	               block_reward: p.block_reward.map_or_else(
	                   || {
	                       let mut ret = BTreeMap::new();
	                       ret.insert(0, U256::zero());
	                       ret
	                   },
	                   |reward| match reward {
	                       ethjson::spec::BlockReward::Single(reward) => {
	                           let mut ret = BTreeMap::new();
	                           ret.insert(0, reward.into());
	                           ret
	                       }
	                       ethjson::spec::BlockReward::Multi(mut multi) => {
	                           if multi.is_empty() {
	                               panic!("No block rewards are found in config");
	                           }
	                           // add block reward from genesis and put reward to zero.
	                           multi
	                               .entry(Uint(U256::from(0)))
	                               .or_insert(Uint(U256::from(0)));
	                           multi
	                               .into_iter()
	                               .map(|(block, reward)| (block.into(), reward.into()))
	                               .collect()
	                       }
	                   },
	               ),
	               block_reward_contract_transitions: br_transitions,
	               maximum_uncle_count_transition: p.maximum_uncle_count_transition.map_or(0, Into::into),
	               maximum_uncle_count: p.maximum_uncle_count.map_or(0, Into::into),
	               empty_steps_transition: p
	                   .empty_steps_transition
	                   .map_or(u64::max_value(), |n| ::std::cmp::max(n.into(), 1)),
	               maximum_empty_steps: p.maximum_empty_steps.map_or(0, Into::into),
	               two_thirds_majority_transition: p
	                   .two_thirds_majority_transition
	                   .map_or_else(BlockNumber::max_value, Into::into),
	               strict_empty_steps_transition: p.strict_empty_steps_transition.map_or(0, Into::into),
	               randomness_contract_address,
	               block_gas_limit_contract_transitions,
	               posdao_transition: p.posdao_transition.map(Into::into),
	           }
	       }
	   }
	*/
	return AuthorityRoundParams{}
}
