package aurainterfaces

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus"
)

// see openethereum/crates/ethcore/res/contracts/block_reward.json
type BlockRewardABI interface {
	Reward(benefactors []libcommon.Address, kind []consensus.RewardKind) ([]libcommon.Address, []*uint256.Int, error)
}

type abiDecoder func([]byte, interface{}) error

// see openethereum/crates/ethcore/res/contracts/validator_set.json
type ValidatorSetABI interface {
	GetValidators() ([]byte, abiDecoder)
	ShouldValidatorReport(ourAddr, maliciousValidatorAddress libcommon.Address, blockNum uint64) ([]byte, abiDecoder)
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
