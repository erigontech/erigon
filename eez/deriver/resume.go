// Copyright 2026 The Erigon Authors
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
package deriver

import (
	"context"
	"errors"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
)

// FindResumePoint searches L1 from head back to deployBlock for the newest
// settlement of rollupID whose root is a block of the local chain, the place
// to resume when no checkpoint is usable. canonical returns the number of
// the local canonical block with a hash. It returns nil when no settlement
// matches.
func FindResumePoint(ctx context.Context, reader l1.ChainReader, registry common.Address, rollupID, deployBlock, head uint64,
	canonical func(context.Context, common.Hash) (uint64, bool, error),
) (*Record, error) {
	topics := [][]common.Hash{{protocol.L2ExecutionPerformedTopic}, {common.BigToHash(new(big.Int).SetUint64(rollupID))}}
	chunk := uint64(l1.LogChunk)
	for hi := head; hi >= deployBlock; {
		lo := max(deployBlock, hi-min(hi, chunk-1))
		logs, err := reader.Logs(ctx, lo, hi, registry, topics)
		if errors.Is(err, l1.ErrRangeTooWide) && hi > lo {
			chunk = max(1, chunk/2)
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, l := range slices.Backward(logs) {
			e, err := protocol.ParseL2ExecutionPerformed(l)
			if err != nil {
				return nil, err
			}
			n, ok, err := canonical(ctx, e.NewRoot)
			if err != nil {
				return nil, err
			}
			if ok {
				return &Record{L1Block: uint64(l.BlockNumber), L1BlockHash: l.BlockHash, TxHash: l.TxHash, LastL2Block: n}, nil
			}
		}
		if lo == deployBlock {
			break
		}
		hi = lo - 1
	}
	return nil, nil
}
