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

package verify

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	datadir2 "github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func blocksIO(db kv.RoDB) (services.FullBlockReader, *blockio.BlockWriter) {
	dirs := datadir2.New(filepath.Dir(db.(*mdbx.MdbxKV).Path()))
	br := freezeblocks.NewBlockReader(freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{Enabled: false}, dirs.Snap, 0, log.New()), nil /* BorSnapshots */)
	bw := blockio.NewBlockWriter()
	return br, bw
}

func ValidateTxLookups(chaindata string, logger log.Logger) error {
	db := mdbx.MustOpen(chaindata)
	br, _ := blocksIO(db)
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ch := make(chan os.Signal, 1)
	quitCh := make(chan struct{})
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		close(quitCh)
	}()
	t := time.Now()
	defer func() {
		logger.Info("Validation ended", "it took", time.Since(t))
	}()
	var blockNum uint64
	iterations := 0
	var interrupt bool
	// Validation Process
	blockBytes := big.NewInt(0)
	ctx := context.Background()
	for !interrupt {
		if err := libcommon.Stopped(quitCh); err != nil {
			return err
		}
		blockHash, err := br.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		body, err := br.BodyWithTransactions(ctx, tx, blockHash, blockNum)
		if err != nil {
			return err
		}

		if body == nil {
			logger.Error("Empty body", "blocknum", blockNum)
			break
		}
		blockBytes.SetUint64(blockNum)
		bn := blockBytes.Bytes()

		for _, txn := range body.Transactions {
			val, err := tx.GetOne(kv.TxLookup, txn.Hash().Bytes())
			iterations++
			if iterations%100000 == 0 {
				logger.Info("Validated", "entries", iterations, "number", blockNum)

			}
			if !bytes.Equal(val, bn) {
				if err != nil {
					panic(err)
				}
				panic(fmt.Sprintf("Validation process failed(%d). Expected %b, got %b", iterations, bn, val))
			}
		}
		blockNum++
	}
	return nil
}
