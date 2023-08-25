package execution_client

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
)

var errContextExceeded = "rpc error: code = DeadlineExceeded desc = context deadline exceeded"

// ExecutionEngine is used only for syncing up very close to chain tip and to stay in sync.
// It pretty much mimics engine API.
type ExecutionEngine interface {
	NewPayload(payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash) (bool, error)
	ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error
	SupportInsertion() bool
	InsertBlocks([]*types.Block) error
	InsertBlock(*types.Block) error
	// GetBodiesByRange(start, count uint64) *types.RawBody TODO(Giulio2002): implement
	// GetBodiesByHashes([]) *types.RawBody TODO(Giulio2002): implement
	IsCanonicalHash(libcommon.Hash) (bool, error)
	Ready() (bool, error)
}
