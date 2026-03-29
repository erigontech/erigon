package services

import (
	"github.com/erigontech/erigon/cl/cltypes"
	serviceinterface "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
)

//go:generate mockgen -typed=true -destination=./mock_services/block_service_mock.go -package=mock_services . BlockService
type BlockService serviceinterface.Service[*cltypes.SignedBeaconBlock]

//go:generate mockgen -typed=true -destination=./mock_services/blob_sidecars_service_mock.go -package=mock_services . BlobSidecarsService
type BlobSidecarsService serviceinterface.Service[*cltypes.BlobSidecar]

//go:generate mockgen -typed=true -destination=./mock_services/sync_committee_messages_service_mock.go -package=mock_services . SyncCommitteeMessagesService
type SyncCommitteeMessagesService serviceinterface.Service[*SyncCommitteeMessageForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/sync_contribution_service_mock.go -package=mock_services . SyncContributionService
type SyncContributionService serviceinterface.Service[*SignedContributionAndProofForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/aggregate_and_proof_service_mock.go -package=mock_services . AggregateAndProofService
type AggregateAndProofService serviceinterface.Service[*SignedAggregateAndProofForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/attestation_service_mock.go -package=mock_services . AttestationService
type AttestationService serviceinterface.Service[*AttestationForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/voluntary_exit_service_mock.go -package=mock_services . VoluntaryExitService
type VoluntaryExitService serviceinterface.Service[*SignedVoluntaryExitForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/bls_to_execution_change_service_mock.go -package=mock_services . BLSToExecutionChangeService
type BLSToExecutionChangeService serviceinterface.Service[*SignedBLSToExecutionChangeForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/proposer_slashing_service_mock.go -package=mock_services . ProposerSlashingService
type ProposerSlashingService serviceinterface.Service[*cltypes.ProposerSlashing]

//go:generate mockgen -typed=true -destination=./mock_services/data_column_sidecar_service_mock.go -package=mock_services . DataColumnSidecarService
type DataColumnSidecarService serviceinterface.Service[*cltypes.DataColumnSidecar]

//go:generate mockgen -typed=true -destination=./mock_services/attester_slashing_service_mock.go -package=mock_services . AttesterSlashingService
type AttesterSlashingService serviceinterface.Service[*cltypes.AttesterSlashing]
