package beacon_indexes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

type BeaconIndexer interface {
	BeaconIndexerReader
	BeaconIndexerWriter
}

type BeaconIndexerReader interface {
	ReadBlockSlotByBlockRoot(blockRoot libcommon.Hash) (uint64, error)
	ReadCanonicalBlockRoot(slot uint64) (libcommon.Hash, error)
	ReadParentBlockRoot(blockRoot libcommon.Hash) (libcommon.Hash, error)
}

type BeaconIndexerWriter interface {
	MarkRootCanonical(slot uint64, blockRoot libcommon.Hash) error
	GenerateBlockIndicies(block *cltypes.BeaconBlock) error
}
