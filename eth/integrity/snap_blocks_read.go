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

package integrity

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/turbo/services"
)

func SnapBlocksRead(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, from, to uint64, failFast bool) error {
	defer log.Info("[integrity] Blocks: done")
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	maxBlockNum := blockReader.Snapshots().SegmentsMax()

	if to != 0 && maxBlockNum > to {
		maxBlockNum = 2
	}

	for i := from; i < maxBlockNum; i += 10_000 {
		if err := db.View(ctx, func(tx kv.Tx) error {
			b, err := blockReader.BlockByNumber(ctx, tx, i)
			if err != nil {
				return err
			}
			if b == nil {
				err := fmt.Errorf("[integrity] block not found in snapshots: %d", i)
				if failFast {
					return err
				}
				log.Error(err.Error())
			}
			return nil
		}); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-logEvery.C:
			log.Info("[integrity] Blocks", "blockNum", fmt.Sprintf("%s/%s", common.PrettyCounter(i), common.PrettyCounter(maxBlockNum)))
		default:
		}
	}
	return nil
}
