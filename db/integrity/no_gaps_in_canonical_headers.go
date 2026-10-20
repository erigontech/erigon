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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func NoGapsInCanonicalHeaders(ctx context.Context, db kv.RoDB, br dbservices.FullBlockReader, failFast bool) error {
	var probs problems
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	if err := br.Integrity(ctx, tx); err != nil {
		panic(err)
	}

	firstBlockInDB := br.FrozenBlocks() + 1
	lastBlockNum, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		panic(err)
	}

	for i := firstBlockInDB; i < lastBlockNum; i++ {
		hash, ok, err := br.CanonicalHash(ctx, tx, i)
		if err != nil {
			panic(err)
		}
		if !ok || hash == (common.Hash{}) {
			if err := probs.report(failFast, fmt.Errorf("canonical marker not found: %d", i)); err != nil {
				return err
			}
			// Without a hash there is nothing to read the header and body with, and reading them
			// anyway ends the run on the first gap it was asked to carry on past.
			continue
		}
		header := rawdb.ReadHeader(tx, hash, i)
		if header == nil {
			err = fmt.Errorf("header not found: %d", i)
			panic(err)
		}
		body, _, _ := rawdb.ReadBody(tx, hash, i)
		if body == nil {
			if err := probs.report(failFast, fmt.Errorf("body not found: %d", i)); err != nil {
				return err
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[integrity] HeaderNoGaps", "progress", fmt.Sprintf("%s/%s", common.PrettyExact(i), common.PrettyExact(lastBlockNum)))
		default:
		}
	}
	return probs.verdict(string(HeaderNoGaps))
}
