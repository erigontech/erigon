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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto/blake2b"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const operationsPerPool = 512

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
		AttestationsPool:          NewOperationPool[common.Bytes96, *solid.Attestation](operationsPerPool, "attestationsPool"),
		AttesterSlashingsPool:     NewOperationPool[common.Bytes96, *cltypes.AttesterSlashing](operationsPerPool, "attesterSlashingsPool"),
		ProposerSlashingsPool:     NewOperationPool[common.Bytes96, *cltypes.ProposerSlashing](operationsPerPool, "proposerSlashingsPool"),
		BLSToExecutionChangesPool: NewOperationPool[common.Bytes96, *cltypes.SignedBLSToExecutionChange](operationsPerPool, "blsExecutionChangesPool"),
		VoluntaryExitsPool:        NewOperationPool[uint64, *cltypes.SignedVoluntaryExit](operationsPerPool, "voluntaryExitsPool"),
	}
}

func (o *OperationsPool) NotifyBlock(blk *cltypes.BeaconBlock) {
	blk.Body.VoluntaryExits.Range(func(_ int, exit *cltypes.SignedVoluntaryExit, _ int) bool {
		o.VoluntaryExitsPool.DeleteIfExist(exit.VoluntaryExit.ValidatorIndex)
		return true
	})
	blk.Body.AttesterSlashings.Range(func(_ int, att *cltypes.AttesterSlashing, _ int) bool {
		o.AttesterSlashingsPool.DeleteIfExist(ComputeKeyForAttesterSlashing(att))
		return true
	})
	blk.Body.ProposerSlashings.Range(func(_ int, ps *cltypes.ProposerSlashing, _ int) bool {
		o.ProposerSlashingsPool.DeleteIfExist(ComputeKeyForProposerSlashing(ps))
		return true
	})
	blk.Body.ExecutionChanges.Range(func(_ int, c *cltypes.SignedBLSToExecutionChange, _ int) bool {
		o.BLSToExecutionChangesPool.DeleteIfExist(c.Signature)
		return true
	})
	o.BLSToExecutionChangesPool.pool.Purge()
}
