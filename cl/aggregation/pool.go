package aggregation

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

type AggregationPool interface {
	AddAttestation(att *solid.Attestation) error
	//GetAggregatations(slot uint64, committeeIndex uint64) ([]*solid.Attestation, error)
	GetAggregatationByRoot(root common.Hash) *solid.Attestation
}
