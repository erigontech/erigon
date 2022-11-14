package aurainterfaces

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
)

// see openethereum/crates/ethcore/res/contracts/block_reward.json
type BlockRewardABI interface {
	Reward(benefactors []common.Address, kind []RewardKind) ([]common.Address, []*uint256.Int, error)
}

type abiDecoder func([]byte, interface{}) error

// see openethereum/crates/ethcore/res/contracts/validator_set.json
type ValidatorSetABI interface {
	GetValidators() ([]byte, abiDecoder)
	ShouldValidatorReport(ourAddr, maliciousValidatorAddress common.Address, blockNum uint64) ([]byte, abiDecoder)
}

// RewardKind - The kind of block reward.
// Depending on the consensus engine the allocated block reward might have
// different semantics which could lead e.g. to different reward values.
type RewardKind uint16

const (
	// RewardAuthor - attributed to the block author.
	RewardAuthor RewardKind = 0
	// RewardEmptyStep - attributed to the author(s) of empty step(s) included in the block (AuthorityRound engine).
	RewardEmptyStep RewardKind = 1
	// RewardExternal - attributed by an external protocol (e.g. block reward contract).
	RewardExternal RewardKind = 2
	// RewardUncle - attributed to the block uncle(s) with given difference.
	RewardUncle RewardKind = 3
)

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
