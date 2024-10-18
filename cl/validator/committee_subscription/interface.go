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

package committee_subscription

import (
	"context"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

//go:generate mockgen -typed=true -destination=./mock_services/committee_subscribe_mock.go -package=mock_services . CommitteeSubscribe
type CommitteeSubscribe interface {
	AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error
	AggregateAttestation(att *solid.Attestation) error
	NeedToAggregate(att *solid.Attestation) bool
}
