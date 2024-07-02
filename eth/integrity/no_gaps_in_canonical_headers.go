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

	"github.com/ledgerwatch/erigon-lib/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func NoGapsInCanonicalHeaders(tx kv.Tx, ctx context.Context, br services.FullBlockReader) {
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	if err := br.Integrity(ctx); err != nil {
		panic(err)
	}

	firstBlockInDB := br.FrozenBlocks() + 1
	lastBlockNum, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		panic(err)
	}

	for i := firstBlockInDB; i < lastBlockNum; i++ {
		hash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			panic(err)
		}
		if hash == (common.Hash{}) {
			err = fmt.Errorf("canonical marker not found: %d\n", i)
			panic(err)
		}
		header := rawdb.ReadHeader(tx, hash, i)
		if header == nil {
			err = fmt.Errorf("header not found: %d\n", i)
			panic(err)
		}
		body, _, _ := rawdb.ReadBody(tx, hash, i)
		if body == nil {
			err = fmt.Errorf("header not found: %d\n", i)
			panic(err)
		}

		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			log.Info("[integrity] NoGapsInCanonicalHeaders", "progress", fmt.Sprintf("%dK/%dK", i/1000, lastBlockNum/1000))
		default:
		}
	}
}
