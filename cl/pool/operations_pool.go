package pool

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// DoubleSignatureKey uses blake2b algorithm to merge two signatures together. blake2 is faster than sha3.
func doubleSignatureKey(one, two libcommon.Bytes96) (out libcommon.Bytes96) {
	res := utils.Keccak256(one[:], two[:])
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
	AttestationsPool          *OperationPool[*solid.Attestation]
	AttesterSlashingsPool     *OperationPool[*cltypes.AttesterSlashing]
	ProposerSlashingsPool     *OperationPool[*cltypes.ProposerSlashing]
	BLSToExecutionChangesPool *OperationPool[*cltypes.SignedBLSToExecutionChange]
	VoluntaryExistsPool       *OperationPool[*cltypes.SignedVoluntaryExit]
}

func NewOperationsPool(beaconCfg *clparams.BeaconChainConfig) OperationsPool {
	return OperationsPool{
		AttestationsPool:          NewOperationPool[*solid.Attestation](int(beaconCfg.MaxAttestations), "attestationsPool"),
		AttesterSlashingsPool:     NewOperationPool[*cltypes.AttesterSlashing](int(beaconCfg.MaxAttestations), "attesterSlashingsPool"),
		ProposerSlashingsPool:     NewOperationPool[*cltypes.ProposerSlashing](int(beaconCfg.MaxAttestations), "proposerSlashingsPool"),
		BLSToExecutionChangesPool: NewOperationPool[*cltypes.SignedBLSToExecutionChange](int(beaconCfg.MaxBlsToExecutionChanges), "blsExecutionChangesPool"),
		VoluntaryExistsPool:       NewOperationPool[*cltypes.SignedVoluntaryExit](int(beaconCfg.MaxBlsToExecutionChanges), "voluntaryExitsPool"),
	}
}
