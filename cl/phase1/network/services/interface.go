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

package services

import (
	"context"

	"github.com/erigontech/erigon/cl/cltypes"
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
type SyncCommitteeMessagesService Service[*SyncCommitteeMessageForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/sync_contribution_service_mock.go -package=mock_services . SyncContributionService
type SyncContributionService Service[*SignedContributionAndProofForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/aggregate_and_proof_service_mock.go -package=mock_services . AggregateAndProofService
type AggregateAndProofService Service[*SignedAggregateAndProofForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/attestation_service_mock.go -package=mock_services . AttestationService
type AttestationService Service[*AttestationForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/voluntary_exit_service_mock.go -package=mock_services . VoluntaryExitService
type VoluntaryExitService Service[*SignedVoluntaryExitForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/bls_to_execution_change_service_mock.go -package=mock_services . BLSToExecutionChangeService
type BLSToExecutionChangeService Service[*SignedBLSToExecutionChangeForGossip]

//go:generate mockgen -typed=true -destination=./mock_services/proposer_slashing_service_mock.go -package=mock_services . ProposerSlashingService
type ProposerSlashingService Service[*cltypes.ProposerSlashing]

//go:generate mockgen -typed=true -destination=./mock_services/data_column_sidecar_service_mock.go -package=mock_services . DataColumnSidecarService
type DataColumnSidecarService Service[*cltypes.DataColumnSidecar]
