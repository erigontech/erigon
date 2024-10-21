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

package bridge

import (
	"context"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
)

type PolygonBridge interface {
	PolygonBridgeReader
	InitialBlockReplayNeeded(ctx context.Context) (uint64, bool, error)
	ReplayInitialBlock(ctx context.Context, block *types.Block) error
	ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error
	Synchronize(ctx context.Context, blockNum uint64) error
	Unwind(ctx context.Context, blockNum uint64) error
}

type PolygonBridgeReader interface {
	LastProcessedBlock(ctx context.Context) (uint64, error)
	Events(ctx context.Context, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error)
}

type Service interface {
	PolygonBridge
	Run(ctx context.Context) error
	Ready(ctx context.Context) <-chan error
}

type ReaderService interface {
	PolygonBridgeReader
	Close()
}
