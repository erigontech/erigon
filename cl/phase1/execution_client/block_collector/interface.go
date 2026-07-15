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

package block_collector

import (
	"context"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/db/kv/dbutils"
)

var batchSize = 1000

type BlockCollector interface {
	AddBlock(block *cltypes.BeaconBlock) error
	// AddGloasBlock adds a GLOAS (EIP-7732) FULL block using its execution payload envelope.
	AddGloasBlock(block *cltypes.BeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) error
	Flush(ctx context.Context) error
	HasBlock(blockNumber uint64) bool
}

// payloadKey returns the key for the payload: just the block number.
// Using only the block number ensures that reorged blocks overwrite
// the previous entry, keeping the collector in sync with the canonical chain.
func payloadKey(payload *cltypes.Eth1Block) ([]byte, error) {
	return dbutils.EncodeBlockNumber(payload.BlockNumber), nil
}
