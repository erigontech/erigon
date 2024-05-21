package optimistic

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

type OptimisticStore interface {
	AddOptimisticCandidate(block *cltypes.BeaconBlock) error
	ValidateBlock(block *cltypes.BeaconBlock) error
	InvalidateBlock(block *cltypes.BeaconBlock) error
	IsOptimistic(root common.Hash) bool
}
