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

package pool

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/blake2b"
)

// Pool capacities are gossip retention windows bounded by finalized-state pruning and eviction.
const (
	// Aggregates are retained for this many slots. A deeper window offers the proposer more
	// partial aggregates to merge, but block production is linear in pool occupancy and
	// quadratic within one data root, with a BLS aggregation per merge, so widening it is a
	// latency trade and needs measuring first.
	attestationRetentionSlots = 10
	attesterSlashingsCapacity = 10240
	proposerSlashingsCapacity = 10240
	// Voluntary exits are keyed by validator index and sized for a mass-exit queue.
	voluntaryExitsCapacity = 16384
	// BLS changes remain available through reorgs until their credential changes are finalized.
	blsToExecutionChangesCapacity = 10240
)

func attestationsCapacity(beaconCfg *clparams.BeaconChainConfig) int {
	return attestationRetentionSlots * int(beaconCfg.MaxCommitteesPerSlot*beaconCfg.TargetAggregatorsPerCommittee)
}

// DoubleSignatureKey uses blake2b algorithm to merge two signatures together. blake2 is faster than sha3.
func doubleSignatureKey(one, two common.Bytes96) (out common.Bytes96) {
	res := blake2b.Sum256(append(one[:], two[:]...))
	copy(out[:], res[:])
	return
}

func ComputeKeyForProposerSlashing(slashing *cltypes.ProposerSlashing) common.Bytes96 {
	return doubleSignatureKey(slashing.Header1.Signature, slashing.Header2.Signature)
}

func ComputeKeyForAttesterSlashing(slashing *cltypes.AttesterSlashing) common.Bytes96 {
	return doubleSignatureKey(slashing.Attestation_1.Signature, slashing.Attestation_2.Signature)
}

// OperationsPool is the collection of all gossip-collectable operations.
type OperationsPool struct {
	AttestationsPool          *OperationPool[common.Bytes96, *solid.Attestation]
	AttesterSlashingsPool     *OperationPool[common.Bytes96, *cltypes.AttesterSlashing]
	ProposerSlashingsPool     *OperationPool[common.Bytes96, *cltypes.ProposerSlashing]
	BLSToExecutionChangesPool *OperationPool[common.Bytes96, *cltypes.SignedBLSToExecutionChange]
	VoluntaryExitsPool        *OperationPool[uint64, *cltypes.SignedVoluntaryExit]
}

func NewOperationsPool(beaconCfg *clparams.BeaconChainConfig) OperationsPool {
	return OperationsPool{
		AttestationsPool:          NewOperationPool[common.Bytes96, *solid.Attestation](attestationsCapacity(beaconCfg), "attestationsPool"),
		AttesterSlashingsPool:     NewOperationPool[common.Bytes96, *cltypes.AttesterSlashing](attesterSlashingsCapacity, "attesterSlashingsPool"),
		ProposerSlashingsPool:     NewOperationPool[common.Bytes96, *cltypes.ProposerSlashing](proposerSlashingsCapacity, "proposerSlashingsPool"),
		BLSToExecutionChangesPool: NewOperationPool[common.Bytes96, *cltypes.SignedBLSToExecutionChange](blsToExecutionChangesCapacity, "blsExecutionChangesPool"),
		VoluntaryExitsPool:        NewOperationPool[uint64, *cltypes.SignedVoluntaryExit](voluntaryExitsCapacity, "voluntaryExitsPool"),
	}
}

func (o *OperationsPool) HasPrunableOperations() bool {
	return o.AttesterSlashingsPool.Len() > 0 ||
		o.ProposerSlashingsPool.Len() > 0 ||
		o.BLSToExecutionChangesPool.Len() > 0 ||
		o.VoluntaryExitsPool.Len() > 0
}

func (o *OperationsPool) PruneFinalized(finalizedState abstract.BeaconState, finalizedEpoch uint64) {
	if finalizedState == nil {
		return
	}

	for _, slashing := range o.ProposerSlashingsPool.Raw() {
		if slashing == nil || slashing.Header1 == nil || slashing.Header1.Header == nil || slashing.Header2 == nil {
			continue
		}
		validator, ok := validatorFromState(finalizedState, slashing.Header1.Header.ProposerIndex)
		if ok && slashingTerminal(validator, finalizedEpoch) {
			o.ProposerSlashingsPool.DeleteIfExist(ComputeKeyForProposerSlashing(slashing))
		}
	}

	for _, slashing := range o.AttesterSlashingsPool.Raw() {
		if !attesterSlashingTerminal(finalizedState, slashing, finalizedEpoch) {
			continue
		}
		o.AttesterSlashingsPool.DeleteIfExist(ComputeKeyForAttesterSlashing(slashing))
	}

	for _, exit := range o.VoluntaryExitsPool.Raw() {
		if exit == nil || exit.VoluntaryExit == nil {
			continue
		}
		validatorIndex := exit.VoluntaryExit.ValidatorIndex
		// Builder indices are reusable, so finalized validator state cannot prove a builder exit terminal.
		if validatorIndex&clparams.BuilderIndexFlag != 0 {
			continue
		}
		validator, ok := validatorFromState(finalizedState, validatorIndex)
		if ok && validator.ExitEpoch() != finalizedState.BeaconConfig().FarFutureEpoch {
			o.VoluntaryExitsPool.DeleteIfExist(validatorIndex)
		}
	}

	for _, change := range o.BLSToExecutionChangesPool.Raw() {
		if change == nil || change.Message == nil {
			continue
		}
		validator, ok := validatorFromState(finalizedState, change.Message.ValidatorIndex)
		if ok && validator.WithdrawalCredentials()[0] != byte(finalizedState.BeaconConfig().BLSWithdrawalPrefixByte) {
			o.BLSToExecutionChangesPool.DeleteIfExist(change.Signature)
		}
	}
}

func validatorFromState(finalizedState abstract.BeaconState, validatorIndex uint64) (solid.Validator, bool) {
	if validatorIndex >= uint64(finalizedState.ValidatorLength()) {
		return nil, false
	}
	validator, err := finalizedState.ValidatorForValidatorIndex(int(validatorIndex))
	return validator, err == nil && validator != nil
}

func slashingTerminal(validator solid.Validator, finalizedEpoch uint64) bool {
	return validator.Slashed() || validator.WithdrawableEpoch() <= finalizedEpoch
}

func attesterSlashingTerminal(finalizedState abstract.BeaconState, slashing *cltypes.AttesterSlashing, finalizedEpoch uint64) bool {
	if slashing == nil || slashing.Attestation_1 == nil || slashing.Attestation_2 == nil ||
		slashing.Attestation_1.AttestingIndices == nil || slashing.Attestation_2.AttestingIndices == nil {
		return false
	}
	intersection := solid.IntersectionOfSortedSets(
		slashing.Attestation_1.AttestingIndices,
		slashing.Attestation_2.AttestingIndices,
	)
	if len(intersection) == 0 {
		return false
	}
	for _, validatorIndex := range intersection {
		validator, ok := validatorFromState(finalizedState, validatorIndex)
		if !ok || !slashingTerminal(validator, finalizedEpoch) {
			return false
		}
	}
	return true
}
