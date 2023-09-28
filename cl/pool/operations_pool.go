package pool

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/crypto/blake2b"
)

// DoubleSignatureKey uses blake2b algorithm to merge two signatures together. blake2 is faster than sha3.
func doubleSignatureKey(one, two libcommon.Bytes96) (out libcommon.Bytes96) {
	res := blake2b.Sum256(append(one[:], two[:]...))
	copy(out[:], res[:])
	return
}

func ComputeKeyForProposerSlashing(slashing *cltypes.ProposerSlashing) libcommon.Bytes96 {
	return doubleSignatureKey(slashing.Header1.Signature, slashing.Header2.Signature)
}

func ComputeKeyForAttesterSlashing(slashing *cltypes.AttesterSlashing) libcommon.Bytes96 {
	return doubleSignatureKey(slashing.Attestation_1.Signature, slashing.Attestation_2.Signature)
}

// OperationsPool is the collection of all gossip-collectable operations.
type OperationsPool struct {
	AttestationsPool          *OperationPool[libcommon.Bytes96, *solid.Attestation]
	AttesterSlashingsPool     *OperationPool[libcommon.Bytes96, *cltypes.AttesterSlashing]
	ProposerSlashingsPool     *OperationPool[libcommon.Bytes96, *cltypes.ProposerSlashing]
	BLSToExecutionChangesPool *OperationPool[libcommon.Bytes96, *cltypes.SignedBLSToExecutionChange]
	VoluntaryExistsPool       *OperationPool[uint64, *cltypes.SignedVoluntaryExit]
}

func NewOperationsPool(beaconCfg *clparams.BeaconChainConfig) OperationsPool {
	return OperationsPool{
		AttestationsPool:          NewOperationPool[libcommon.Bytes96, *solid.Attestation](int(beaconCfg.MaxAttestations), "attestationsPool"),
		AttesterSlashingsPool:     NewOperationPool[libcommon.Bytes96, *cltypes.AttesterSlashing](int(beaconCfg.MaxAttestations), "attesterSlashingsPool"),
		ProposerSlashingsPool:     NewOperationPool[libcommon.Bytes96, *cltypes.ProposerSlashing](int(beaconCfg.MaxAttestations), "proposerSlashingsPool"),
		BLSToExecutionChangesPool: NewOperationPool[libcommon.Bytes96, *cltypes.SignedBLSToExecutionChange](int(beaconCfg.MaxBlsToExecutionChanges), "blsExecutionChangesPool"),
		VoluntaryExistsPool:       NewOperationPool[uint64, *cltypes.SignedVoluntaryExit](int(beaconCfg.MaxBlsToExecutionChanges), "voluntaryExitsPool"),
	}
}

func (o *OperationsPool) NotifyBlock(blk *cltypes.BeaconBlock) {
	blk.Body.VoluntaryExits.Range(func(_ int, exit *cltypes.SignedVoluntaryExit, _ int) bool {
		o.VoluntaryExistsPool.DeleteIfExist(exit.VoluntaryExit.ValidatorIndex)
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
}
