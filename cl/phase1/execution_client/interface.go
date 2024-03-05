package execution_client

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
)

var errContextExceeded = "rpc error: code = DeadlineExceeded desc = context deadline exceeded"

// ExecutionEngine is used only for syncing up very close to chain tip and to stay in sync.
// It pretty much mimics engine API.
type ExecutionEngine interface {
	NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash, versionedHashes []libcommon.Hash) (bool, error)
	ForkChoiceUpdate(ctx context.Context, finalized libcommon.Hash, head libcommon.Hash) error
	SupportInsertion() bool
	InsertBlocks(ctx context.Context, blocks []*types.Block, wait bool) error
	InsertBlock(ctx context.Context, block *types.Block) error
	IsCanonicalHash(ctx context.Context, hash libcommon.Hash) (bool, error)
	Ready(ctx context.Context) (bool, error)
	// Range methods
	GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error)
	GetBodiesByHashes(ctx context.Context, hashes []libcommon.Hash) ([]*types.RawBody, error)
	// Snapshots
	FrozenBlocks(ctx context.Context) uint64
}
