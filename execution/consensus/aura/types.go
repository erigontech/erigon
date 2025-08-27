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

package aura

import (
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/types"
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
	ContractAddress common.Address
	Header          *types.Header
}

type EpochTransition struct {
	/// Block hash at which the transition occurred.
	BlockHash common.Hash
	/// Block number at which the transition occurred.
	BlockNumber uint64
	/// "transition/epoch" proof from the engine combined with a finality proof.
	ProofRlp []byte
}

type epochReader interface {
	GetEpoch(blockHash common.Hash, blockN uint64) (transitionProof []byte, err error)
	GetPendingEpoch(blockHash common.Hash, blockN uint64) (transitionProof []byte, err error)
	FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash common.Hash, transitionProof []byte, err error)
}
type epochWriter interface {
	epochReader
	PutEpoch(blockHash common.Hash, blockN uint64, transitionProof []byte) (err error)
	PutPendingEpoch(blockHash common.Hash, blockN uint64, transitionProof []byte) (err error)
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
