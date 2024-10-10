package committee_subscription

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

//go:generate mockgen -typed=true -destination=./mock_services/committee_subscribe_mock.go -package=mock_services . CommitteeSubscribe
type CommitteeSubscribe interface {
	AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error
	CheckAggregateAttestation(att *solid.Attestation) error
}
