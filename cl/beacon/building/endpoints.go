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

package building

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
)

type BeaconCommitteeSubscription struct {
	ValidatorIndex   int  `json:"validator_index,string"`
	CommitteeIndex   int  `json:"committee_index,string"`
	CommitteesAtSlot int  `json:"committees_at_slot,string"`
	Slot             int  `json:"slot,string"`
	IsAggregator     bool `json:"is_aggregator"`
}

type SyncCommitteeSubscription struct {
	ValidatorIndex       int                 `json:"validator_index,string"`
	SyncCommitteeIndices []beaconhttp.IntStr `json:"sync_committee_indices"`
	UntilEpoch           int                 `json:"until_epoch,string"`
}

type PrepareBeaconProposer struct {
	ValidatorIndex int            `json:"validator_index,string"`
	FeeRecipient   common.Address `json:"fee_recipient"`
}
