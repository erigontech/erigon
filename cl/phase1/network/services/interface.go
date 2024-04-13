package services

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// Note: BlobSidecarService and BlockService are tested in spectests

type Service[T any] interface {
	ProcessMessage(ctx context.Context, subnet *uint64, msg T) error
}

//go:generate mockgen -destination=./mock_services/block_service_mock.go -package=mock_services . BlockService
type BlockService Service[*cltypes.SignedBeaconBlock]

//go:generate mockgen -destination=./mock_services/blob_sidecars_service_mock.go -package=mock_services . BlobSidecarsService
type BlobSidecarsService Service[*cltypes.BlobSidecar]

//go:generate mockgen -destination=./mock_services/sync_committee_messages_service_mock.go -package=mock_services . SyncCommitteeMessagesService
type SyncCommitteeMessagesService Service[*cltypes.SyncCommitteeMessage]

//go:generate mockgen -destination=./mock_services/sync_contribution_service_mock.go -package=mock_services . SyncContributionService
type SyncContributionService Service[*cltypes.SignedContributionAndProof]

//go:generate mockgen -destination=./mock_services/aggregate_and_proof_service_mock.go -package=mock_services . AggregateAndProofService
type AggregateAndProofService Service[*cltypes.SignedAggregateAndProof]
