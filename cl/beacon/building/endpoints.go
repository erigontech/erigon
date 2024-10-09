package building

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
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
