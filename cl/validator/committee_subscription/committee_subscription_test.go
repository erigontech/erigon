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

	"github.com/erigontech/erigon/cl/aggregation/mock_services"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// The REST layer turns this into 503 CurrentlySyncing by matching the sentinel, so a bare error
// here would be reported as an internal failure instead.
func TestAddAttestationSubscriptionReportsNotSyncedWhileSyncing(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	c := &CommitteeSubscribeMgmt{
		beaconConfig: &cfg,
		syncedData:   synced_data.NewSyncedDataManager(&cfg, true),
	}

	err := c.AddAttestationSubscription(t.Context(), &cltypes.BeaconCommitteeSubscription{
		ValidatorIndex:   1,
		CommitteeIndex:   0,
		CommitteesAtSlot: 1,
		Slot:             1,
	})

	require.ErrorIs(t, err, synced_data.ErrNotSynced)
}

// Block production packs from the aggregation pool, so every verified attestation is kept,
// not only those of committees with a local aggregator.
func TestAggregateAttestationKeepsAttestationsWithoutLocalAggregator(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.ElectraForkEpoch = 0
	pool := mock_services.NewMockAggregationPool(gomock.NewController(t))
	c := &CommitteeSubscribeMgmt{
		beaconConfig:    &cfg,
		aggregationPool: pool,
		validatorSubs:   map[uint64]*validatorSub{},
	}
	committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
	require.NoError(t, committeeBits.SetBitAt(3, true))
	att := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000011}, int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot)),
		Data:            &solid.AttestationData{Slot: 100},
		CommitteeBits:   committeeBits,
	}
	pool.EXPECT().AddAttestation(att).Return(nil).Times(1)

	require.NoError(t, c.AggregateAttestation(att))
}
