// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execution_client

import (
	"context"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/engineapi/engine_types"
)

var errContextExceeded = "rpc error: code = DeadlineExceeded desc = context deadline exceeded"

// ExecutionEngine is used only for syncing up very close to chain tip and to stay in sync.
// It pretty much mimics engine API.

//go:generate mockgen -typed=true -source=./interface.go -destination=./execution_engine_mock.go -package=execution_client . ExecutionEngine
type ExecutionEngine interface {
	NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash, versionedHashes []libcommon.Hash) (PayloadStatus, error)
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
	HasGapInSnapshots(ctx context.Context) bool
	// Block production
	GetAssembledBlock(ctx context.Context, id []byte) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *big.Int, error)
}
