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

package aggregation

import (
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

//go:generate mockgen -typed=true -destination=./mock_services/aggregation_pool_mock.go -package=mock_services . AggregationPool
type AggregationPool interface {
	// AddAttestation adds a single attestation to the pool.
	AddAttestation(att *solid.Attestation) error
	GetAggregatationByRoot(root common.Hash) *solid.Attestation
	// GetAggregatationByRootAndCommittee returns the committee's aggregate of the data, also when the
	// pool keeps it aside because the committee is full.
	GetAggregatationByRootAndCommittee(root common.Hash, slot, committeeIndex uint64) *solid.Attestation
	// Aggregates returns the current per-committee aggregates (Electra and later). The pool replaces
	// an aggregate instead of changing it, so callers may keep them but must not modify them.
	Aggregates() []*solid.Attestation
	// PinAttestationData marks attestation data the node serves its validators. Once it is stored,
	// gossip votes for other data cannot displace it.
	PinAttestationData(data *solid.AttestationData) error
	// AddAggregatorDuty marks a committee with a local aggregator, whose attestation data may come from
	// another beacon node. Votes for data the committee does not store are then kept for every voter.
	AddAggregatorDuty(slot, committeeIndex uint64)
}
