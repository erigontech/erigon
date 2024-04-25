package aggregation

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

//go:generate mockgen -destination=./mock_services/aggregation_pool_mock.go -package=mock_services . AggregationPool
type AggregationPool interface {
	AddAttestation(att *solid.Attestation) error
	//GetAggregatations(slot uint64, committeeIndex uint64) ([]*solid.Attestation, error)
	GetAggregatationByRoot(root common.Hash) *solid.Attestation
}
