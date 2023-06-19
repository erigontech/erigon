package aura

import (
	"sync/atomic"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
)

type StepDurationInfo struct {
	TransitionStep      uint64
	TransitionTimestamp uint64
	StepDuration        uint64
}

// EpochTransitionProof - Holds 2 proofs inside: ValidatorSetProof and FinalityProof
type EpochTransitionProof struct {
	SignalNumber  uint64
	SetProof      []byte
	FinalityProof []byte
}

// ValidatorSetProof - validator set proof
type ValidatorSetProof struct {
	Header   *types.Header
	Receipts types.Receipts
}

// FirstValidatorSetProof state-dependent proofs for the safe contract:
// only "first" proofs are such.
type FirstValidatorSetProof struct { // TODO: whaaat? here is no state!
	ContractAddress libcommon.Address
	Header          *types.Header
}

type EpochTransition struct {
	/// Block hash at which the transition occurred.
	BlockHash libcommon.Hash
	/// Block number at which the transition occurred.
	BlockNumber uint64
	/// "transition/epoch" proof from the engine combined with a finality proof.
	ProofRlp []byte
}

type epochReader interface {
	GetEpoch(blockHash libcommon.Hash, blockN uint64) (transitionProof []byte, err error)
	GetPendingEpoch(blockHash libcommon.Hash, blockN uint64) (transitionProof []byte, err error)
	FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash libcommon.Hash, transitionProof []byte, err error)
}
type epochWriter interface {
	epochReader
	PutEpoch(blockHash libcommon.Hash, blockN uint64, transitionProof []byte) (err error)
	PutPendingEpoch(blockHash libcommon.Hash, blockN uint64, transitionProof []byte) (err error)
}

type PermissionedStep struct {
	inner      *Step
	canPropose atomic.Bool
}

// An empty step message that is included in a seal, the only difference is that it doesn't include
// the `parent_hash` in order to save space. The included signature is of the original empty step
// message, which can be reconstructed by using the parent hash of the block in which this sealed
// empty message is included.
// nolint
type SealedEmptyStep struct {
	signature []byte // H520
	step      uint64
}
