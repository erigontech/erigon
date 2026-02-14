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
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/dbutils"
)

var batchSize = 1000

type BlockCollector interface {
	AddBlock(block *cltypes.BeaconBlock) error
	Flush(ctx context.Context) error
	HasBlock(blockNumber uint64) bool
}

// serializes block value
func encodeBlock(payload *cltypes.Eth1Block, parentRoot common.Hash, executionRequestsList []hexutil.Bytes) ([]byte, error) {
	encodedPayload, err := payload.EncodeSSZ(nil)
	if err != nil {
		return nil, fmt.Errorf("error encoding execution payload during download: %s", err)
	}
	if executionRequestsList != nil {
		// electra version
		requestsHash := cltypes.ComputeExecutionRequestHash(executionRequestsList)
		// version + parentRoot + requestsHash + encodedPayload
		buf := make([]byte, 1+32+32+len(encodedPayload))
		buf[0] = byte(payload.Version())
		copy(buf[1:], parentRoot[:])
		copy(buf[33:], requestsHash[:])
		copy(buf[65:], encodedPayload)
		return utils.CompressSnappy(buf), nil
	}
	// Use snappy compression that the temporary files do not take too much disk.
	// version + parentRoot + encodedPayload
	buf := make([]byte, 1+32+len(encodedPayload))
	buf[0] = byte(payload.Version())
	copy(buf[1:], parentRoot[:])
	copy(buf[33:], encodedPayload)
	return utils.CompressSnappy(buf), nil
}

// payloadKey returns the key for the payload: number + payload.HashTreeRoot()
func payloadKey(payload *cltypes.Eth1Block) ([]byte, error) {
	root, err := payload.HashSSZ()
	if err != nil {
		return nil, err
	}
	numberBytes := dbutils.EncodeBlockNumber(payload.BlockNumber)
	return append(numberBytes, root[:]...), nil
}
