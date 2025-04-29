package cltypes

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

type GenericBeaconBlock interface {
	Version() clparams.StateVersion
	GetSlot() uint64
	GetProposerIndex() uint64
	GetParentRoot() common.Hash
	GetBody() GenericBeaconBody
}

type GenericBeaconBody interface {
	HashSSZ() ([32]byte, error)
	GetPayloadHeader() (*Eth1Header, error)
	GetRandaoReveal() common.Bytes96
	GetEth1Data() *Eth1Data
	GetSyncAggregate() *SyncAggregate

	GetProposerSlashings() *solid.ListSSZ[*ProposerSlashing]
	GetAttesterSlashings() *solid.ListSSZ[*AttesterSlashing]
	GetAttestations() *solid.ListSSZ[*solid.Attestation]
	GetDeposits() *solid.ListSSZ[*Deposit]
	GetVoluntaryExits() *solid.ListSSZ[*SignedVoluntaryExit]
	GetBlobKzgCommitments() *solid.ListSSZ[*KZGCommitment]
	GetExecutionChanges() *solid.ListSSZ[*SignedBLSToExecutionChange]
	GetExecutionRequests() *ExecutionRequests
}
