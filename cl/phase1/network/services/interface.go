package services

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

// Note: BlobSidecarService and BlockService are tested in spectests

type Service[T any] interface {
	ProcessMessage(ctx context.Context, subnet *uint64, msg T) error
}

//go:generate mockgen -typed=true -destination=./mock_services/block_service_mock.go -package=mock_services . BlockService
type BlockService Service[*cltypes.SignedBeaconBlock]

//go:generate mockgen -typed=true -destination=./mock_services/blob_sidecars_service_mock.go -package=mock_services . BlobSidecarsService
type BlobSidecarsService Service[*cltypes.BlobSidecar]

//go:generate mockgen -typed=true -destination=./mock_services/sync_committee_messages_service_mock.go -package=mock_services . SyncCommitteeMessagesService
type SyncCommitteeMessagesService Service[*cltypes.SyncCommitteeMessage]

//go:generate mockgen -typed=true -destination=./mock_services/sync_contribution_service_mock.go -package=mock_services . SyncContributionService
type SyncContributionService Service[*cltypes.SignedContributionAndProof]

//go:generate mockgen -typed=true -destination=./mock_services/aggregate_and_proof_service_mock.go -package=mock_services . AggregateAndProofService
type AggregateAndProofService Service[*cltypes.SignedAggregateAndProof]

//go:generate mockgen -typed=true -destination=./mock_services/attestation_service_mock.go -package=mock_services . AttestationService
type AttestationService Service[*solid.Attestation]

//go:generate mockgen -typed=true -destination=./mock_services/voluntary_exit_service_mock.go -package=mock_services . VoluntaryExitService
type VoluntaryExitService Service[*cltypes.SignedVoluntaryExit]

//go:generate mockgen -typed=true -destination=./mock_services/bls_to_execution_change_service_mock.go -package=mock_services . BLSToExecutionChangeService
type BLSToExecutionChangeService Service[*cltypes.SignedBLSToExecutionChange]

//go:generate mockgen -typed=true -destination=./mock_services/proposer_slashing_service_mock.go -package=mock_services . ProposerSlashingService
type ProposerSlashingService Service[*cltypes.ProposerSlashing]
