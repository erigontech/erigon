package cltypes

type BeaconCommitteeSubscription struct {
	ValidatorIndex   uint64 `json:"validator_index,string"`
	CommitteeIndex   uint64 `json:"committee_index,string"`
	CommitteesAtSlot uint64 `json:"committees_at_slot,string"`
	Slot             uint64 `json:"slot,string"`
	IsAggregator     bool   `json:"is_aggregator"`
}
