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

package txpoolwait

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

func WaitForParent(ctx context.Context, db kv.RoDB, blockReader dbservices.FullBlockReader, pool *txpool.TxPool, parentHash common.Hash) error {
	if pool == nil {
		return nil
	}
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	header, err := blockReader.HeaderByHash(ctx, tx, parentHash)
	tx.Rollback()
	if err != nil || header == nil {
		return err
	}

	parentBlockNum := header.Number.Uint64()
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, err = pool.PeekBest(waitCtx, 0, &txpool.TxnsRlp{}, parentBlockNum)
	if err != nil {
		return fmt.Errorf("wait for txpool to reach parent block %d: %w", parentBlockNum, err)
	}
	return nil
}
