package execution_client

import (
	"context"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

var errContextExceeded = "rpc error: code = DeadlineExceeded desc = context deadline exceeded"

// ExecutionEngine is used only for syncing up very close to chain tip and to stay in sync.
// It pretty much mimics engine API.
type ExecutionEngine interface {
	NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash, versionedHashes []libcommon.Hash) (bool, error)
	ForkChoiceUpdate(ctx context.Context, finalized libcommon.Hash, head libcommon.Hash, attributes *engine_types.PayloadAttributes) ([]byte, error)
	SupportInsertion() bool
	InsertBlocks(ctx context.Context, blocks []*types.Block, wait bool) error
	InsertBlock(ctx context.Context, block *types.Block) error
	CurrentHeader(ctx context.Context) (*types.Header, error)
	IsCanonicalHash(ctx context.Context, hash libcommon.Hash) (bool, error)
	Ready(ctx context.Context) (bool, error)
	// Range methods
	GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error)
	GetBodiesByHashes(ctx context.Context, hashes []libcommon.Hash) ([]*types.RawBody, error)
	HasBlock(ctx context.Context, hash libcommon.Hash) (bool, error)
	// Snapshots
	FrozenBlocks(ctx context.Context) uint64
	// Block production
	GetAssembledBlock(ctx context.Context, id []byte) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *big.Int, error)
}
