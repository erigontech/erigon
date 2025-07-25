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

package aurainterfaces

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/consensus"
)

// see openethereum/crates/ethcore/res/contracts/block_reward.json
type BlockRewardABI interface {
	Reward(benefactors []common.Address, kind []consensus.RewardKind) ([]common.Address, []*uint256.Int, error)
}

type abiDecoder func([]byte, interface{}) error

// see openethereum/crates/ethcore/res/contracts/validator_set.json
type ValidatorSetABI interface {
	GetValidators() ([]byte, abiDecoder)
	ShouldValidatorReport(ourAddr, maliciousValidatorAddress common.Address, blockNum uint64) ([]byte, abiDecoder)
}

type SealKind [][]byte

// Proposal seal; should be broadcasted, but not inserted into blockchain.
type SealProposal SealKind

// Regular block seal; should be part of the blockchain.
type SealRegular SealKind

// Engine does not generate seal for this block right now.
type None SealKind

// / The type of sealing the engine is currently able to perform.
type SealingState uint8

const (
	/// The engine is ready to seal a block.
	SealingStateReady SealingState = 0
	/// The engine can't seal at the moment, and no block should be prepared and queued.
	SealingStateNotReady SealingState = 1
	/// The engine does not seal internally.
	SealingStateExternal SealingState = 2
)
