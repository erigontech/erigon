package cltypes

import (
	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

type GenericBeaconBlock interface {
	Version() clparams.StateVersion
	GetSlot() uint64
	GetProposerIndex() uint64
	GetParentRoot() libcommon.Hash
	GetBody() GenericBeaconBody
}

type GenericBeaconBody interface {
	HashSSZ() ([32]byte, error)
	GetPayloadHeader() (*Eth1Header, error)
	GetRandaoReveal() libcommon.Bytes96
	GetEth1Data() *Eth1Data
	GetSyncAggregate() *SyncAggregate

	GetProposerSlashings() *solid.ListSSZ[*ProposerSlashing]
	GetAttesterSlashings() *solid.ListSSZ[*AttesterSlashing]
	GetAttestations() *solid.ListSSZ[*solid.Attestation]
	GetDeposits() *solid.ListSSZ[*Deposit]
	GetVoluntaryExits() *solid.ListSSZ[*SignedVoluntaryExit]
	GetBlobKzgCommitments() *solid.ListSSZ[*KZGCommitment]
	GetExecutionChanges() *solid.ListSSZ[*SignedBLSToExecutionChange]
}
