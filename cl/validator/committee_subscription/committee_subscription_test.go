// Copyright 2026 The Erigon Authors
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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	aggregation_mock "github.com/erigontech/erigon/cl/aggregation/mock_services"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

// The REST layer turns this into 503 CurrentlySyncing by matching the sentinel, so a bare error
// here would be reported as an internal failure instead.
func TestAddAttestationSubscriptionReportsNotSyncedWhileSyncing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	c := &CommitteeSubscribeMgmt{
		beaconConfig:    &cfg,
		syncedData:      synced_data.NewSyncedDataManager(&cfg, true),
		aggregationPool: aggregation_mock.NewMockAggregationPool(gomock.NewController(t)),
	}

	err := c.AddAttestationSubscription(t.Context(), &cltypes.BeaconCommitteeSubscription{
		ValidatorIndex:   1,
		CommitteeIndex:   0,
		CommitteesAtSlot: 1,
		Slot:             1,
	})

	require.ErrorIs(t, err, synced_data.ErrNotSynced)
}

// An aggregator subscription tells the pool about the duty, so the pool keeps the committee's data for
// the aggregator; other subscriptions do not.
func TestAddAttestationSubscriptionRegistersAggregatorDuties(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	pool := aggregation_mock.NewMockAggregationPool(gomock.NewController(t))
	pool.EXPECT().AddAggregatorDuty(uint64(40), uint64(3)).Times(1)
	c := &CommitteeSubscribeMgmt{
		beaconConfig:    &cfg,
		syncedData:      synced_data.NewSyncedDataManager(&cfg, true),
		aggregationPool: pool,
	}

	for _, isAggregator := range []bool{true, false} {
		err := c.AddAttestationSubscription(t.Context(), &cltypes.BeaconCommitteeSubscription{
			ValidatorIndex:   1,
			CommitteeIndex:   3,
			CommitteesAtSlot: 4,
			Slot:             40,
			IsAggregator:     isAggregator,
		})
		require.ErrorIs(t, err, synced_data.ErrNotSynced)
	}
}
