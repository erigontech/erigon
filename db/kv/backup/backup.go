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

package backup

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
)

func OpenPair(from, to string, label kv.Label, targetPageSize datasize.ByteSize, logger log.Logger) (kv.RoDB, kv.RwDB) {
	const ThreadsHardLimit = 9_000
	src := mdbx2.New(label, logger).Path(from).
		RoTxsLimiter(semaphore.NewWeighted(ThreadsHardLimit)).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		Accede(true).
		MustOpen()
	if targetPageSize <= 0 {
		targetPageSize = src.PageSize()
	}
	info, err := src.(*mdbx2.MdbxKV).Env().Info(nil)
	if err != nil {
		panic(err)
	}
	dst := mdbx2.New(label, logger).Path(to).
		PageSize(targetPageSize).
		MapSize(datasize.ByteSize(info.Geo.Upper)).
		GrowthStep(4 * datasize.GB).
		WriteMap(true).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TablesCfgByLabel(label) }).
		MustOpen()
	return src, dst
}

func Kv2kv(ctx context.Context, src kv.RoDB, dst kv.RwDB, tables []string, readAheadThreads int, logger log.Logger) error {
	srcTx, err1 := src.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer srcTx.Rollback()

	commitEvery := time.NewTicker(5 * time.Minute)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	tablesMap := src.AllTables()
	if len(tables) > 0 {
		tablesMapCopy := maps.Clone(tablesMap)
		tablesMap = kv.TableCfg{}
		for _, name := range tables {
			tablesMap[name] = tablesMapCopy[name]
		}
	}

	for name, b := range tablesMap {
		if b.IsDeprecated {
			continue
		}
		if err := backupTable(ctx, srcTx, dst, name, logEvery, logger); err != nil {
			return err
		}
	}
	logger.Info("done")
	return nil
}

func backupTable(ctx context.Context, srcTx kv.Tx, dst kv.RwDB, table string, logEvery *time.Ticker, logger log.Logger) error {
	var total uint64
	srcC, err := srcTx.Cursor(table)
	if err != nil {
		return err
	}
	defer srcC.Close()
	total, _ = srcTx.Count(table)

	if err := dst.Update(ctx, func(tx kv.RwTx) error {
		return tx.ClearTable(table)
	}); err != nil {
		return err
	}
	dstTx, err := dst.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer dstTx.Rollback()

	c, err := dstTx.RwCursor(table)
	if err != nil {
		return err
	}
	defer c.Close()
	casted, isDupsort := c.(kv.RwCursorDupSort)
	i := uint64(0)

	for k, v, err := srcC.First(); k != nil; k, v, err = srcC.Next() {
		if err != nil {
			return err
		}

		if isDupsort {
			if err = casted.AppendDup(k, v); err != nil {
				return err
			}
		} else {
			if err = c.Append(k, v); err != nil {
				return err
			}
		}

		i++
		if i%100_000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				logger.Info("Progress", "table", table, "progress",
					fmt.Sprintf("%s/%s", common.PrettyCounter(i), common.PrettyCounter(total)), "key", hex.EncodeToString(k),
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			default:
			}
		}
	}
	// migrate bucket sequences to native mdbx implementation
	//currentID, err := srcTx.Sequence(name, 0)
	//if err != nil {
	//	return err
	//}
	//_, err = dstTx.Sequence(name, currentID)
	//if err != nil {
	//	return err
	//}
	if err2 := dstTx.Commit(); err2 != nil {
		return err2
	}
	return nil
}

const ReadAheadThreads = 2048

func ClearTables(ctx context.Context, tx kv.RwTx, tables ...string) error {
	for _, tbl := range tables {
		log.Info("Clear", "table", tbl)
		if err := tx.ClearTable(tbl); err != nil {
			return err
		}
	}
	return nil
}
