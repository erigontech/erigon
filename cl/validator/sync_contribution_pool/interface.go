package sync_contribution_pool

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

// SyncContributionPool is an interface for managing sync committee contributions and messages.
// it keeps a store of sync committee contributions, if new messages are received they are aggregated with pre-existing contributions.

//go:generate mockgen -typed=true -destination=mock_services/sync_contribution_pool_mock.go -package=mock_services . SyncContributionPool
type SyncContributionPool interface {
	// AddSyncContribution adds a sync committee contribution to the pool.
	AddSyncContribution(headState *state.CachingBeaconState, contribution *cltypes.Contribution) error
	// AddSyncCommitteeMessage aggretates a sync committee message to a contribution to the pool.
	AddSyncCommitteeMessage(headState *state.CachingBeaconState, subCommitee uint64, message *cltypes.SyncCommitteeMessage) error

	// GetSyncContribution retrieves a sync contribution from the pool.
	GetSyncContribution(slot, subcommitteeIndex uint64, beaconBlockRoot common.Hash) *cltypes.Contribution
	// Obtain the sync aggregate for the sync messages pointing to a given beacon block root.
	GetSyncAggregate(slot uint64, beaconBlockRoot common.Hash) (*cltypes.SyncAggregate, error)
}
