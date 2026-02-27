package cltypes

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

// ColumnSyncableSignedBlock is implemented by both SignedBeaconBlock and SignedBlindedBeaconBlock
// for PeerDAS column synchronization operations.
// [New in Gloas:EIP7732] This interface allows PeerDAS to work with both block types
// without needing to call Blinded() which fails for GLOAS blocks.
type ColumnSyncableSignedBlock interface {
	Version() clparams.StateVersion
	GetSlot() uint64
	BlockHashSSZ() ([32]byte, error)
	GetBlobKzgCommitments() *solid.ListSSZ[*KZGCommitment]
}

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
	GetSignedExecutionPayloadBid() *SignedExecutionPayloadBid
	GetPayloadAttestations() *solid.ListSSZ[*PayloadAttestation]
}
