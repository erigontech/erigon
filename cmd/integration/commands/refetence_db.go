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

package commands

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	mdbx2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/turbo/debug"
)

var stateBuckets = []string{
	kv.HashedAccountsDeprecated,
	kv.HashedStorageDeprecated,
	kv.PlainState,
	kv.Code,
	kv.E2AccountsHistory,
	kv.E2StorageHistory,
	kv.TxLookup,
}

var cmdMdbxTopDup = &cobra.Command{
	Use: "mdbx_top_dup",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		err := mdbxTopDup(ctx, chaindata, bucket, logger)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}
var cmdCompareBucket = &cobra.Command{
	Use:   "compare_bucket",
	Short: "compare bucket to the same bucket in '--chaindata.reference'",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		if referenceChaindata == "" {
			referenceChaindata = chaindata + "-copy"
		}
		err := compareBucketBetweenDatabases(ctx, chaindata, referenceChaindata, bucket)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdCompareStates = &cobra.Command{
	Use:   "compare_states",
	Short: "compare state buckets to buckets in '--chaindata.reference'",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		if referenceChaindata == "" {
			referenceChaindata = chaindata + "-copy"
		}
		err := compareStates(ctx, chaindata, referenceChaindata)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdMdbxToMdbx = &cobra.Command{
	Use:   "mdbx_to_mdbx",
	Short: "copy data from '--chaindata' to '--chaindata.to'",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		from, to := backup.OpenPair(chaindata, toChaindata, kv.ChainDB, 0, logger)
		err := backup.Kv2kv(ctx, from, to, nil, backup.ReadAheadThreads, logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdFToMdbx = &cobra.Command{
	Use:   "f_to_mdbx",
	Short: "copy data from '--chaindata' to '--chaindata.to'",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		err := fToMdbx(ctx, logger, toChaindata)
		if err != nil && !errors.Is(err, context.Canceled) {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func init() {
	withDataDir(cmdCompareBucket)
	withReferenceChaindata(cmdCompareBucket)
	withBucket(cmdCompareBucket)

	rootCmd.AddCommand(cmdCompareBucket)

	withDataDir(cmdMdbxTopDup)
	withBucket(cmdMdbxTopDup)

	rootCmd.AddCommand(cmdMdbxTopDup)

	withDataDir(cmdCompareStates)
	withReferenceChaindata(cmdCompareStates)
	withBucket(cmdCompareStates)

	rootCmd.AddCommand(cmdCompareStates)

	withDataDir(cmdMdbxToMdbx)
	withToChaindata(cmdMdbxToMdbx)
	withBucket(cmdMdbxToMdbx)

	rootCmd.AddCommand(cmdMdbxToMdbx)

	withToChaindata(cmdFToMdbx)
	withFile(cmdFToMdbx)
	withBucket(cmdFToMdbx)

	rootCmd.AddCommand(cmdFToMdbx)
}

func mdbxTopDup(ctx context.Context, chaindata string, bucket string, logger log.Logger) error {
	const ThreadsLimit = 5_000
	dbOpts := mdbx2.New(kv.ChainDB, logger).Path(chaindata).Accede(true).RoTxsLimiter(semaphore.NewWeighted(ThreadsLimit)).
		WriteMap(dbWriteMap)

	db := dbOpts.MustOpen()
	defer db.Close()

	cnt := map[string]int{}
	if err := db.View(ctx, func(tx kv.Tx) error {
		c, err := tx.CursorDupSort(bucket)
		if err != nil {
			return err
		}
		defer c.Close()

		for k, _, err := c.First(); k != nil; k, _, err = c.NextNoDup() {
			if err != nil {
				return err
			}
			if _, ok := cnt[string(k)]; !ok {
				cnt[string(k)] = 0
			}
			cnt[string(k)]++
		}
		return nil
	}); err != nil {
		return err
	}

	var _max int
	for _, i := range cnt {
		_max = max(i, _max)
	}
	for k, i := range cnt {
		if i > _max-10 {
			fmt.Printf("k: %x\n", k)
		}
	}

	return nil
}

func compareStates(ctx context.Context, chaindata string, referenceChaindata string) error {
	db := mdbx2.MustOpen(chaindata)
	defer db.Close()

	refDB := mdbx2.MustOpen(referenceChaindata)
	defer refDB.Close()

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		if err := refDB.View(context.Background(), func(refTX kv.Tx) error {
			for _, bucket := range stateBuckets {
				fmt.Printf("\nBucket: %s\n", bucket)
				if err := compareBuckets(ctx, tx, bucket, refTX, bucket); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
func compareBucketBetweenDatabases(ctx context.Context, chaindata string, referenceChaindata string, bucket string) error {
	db := mdbx2.MustOpen(chaindata)
	defer db.Close()

	refDB := mdbx2.MustOpen(referenceChaindata)
	defer refDB.Close()

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		return refDB.View(context.Background(), func(refTX kv.Tx) error {
			return compareBuckets(ctx, tx, bucket, refTX, bucket)
		})
	}); err != nil {
		return err
	}

	return nil
}

func compareBuckets(ctx context.Context, tx kv.Tx, b string, refTx kv.Tx, refB string) error {
	count := 0
	c, err := tx.Cursor(b)
	if err != nil {
		return err
	}
	defer c.Close()
	k, v, e := c.First()
	if e != nil {
		return e
	}
	refC, err := refTx.Cursor(refB)
	if err != nil {
		return err
	}
	defer refC.Close()
	refK, refV, revErr := refC.First()
	if revErr != nil {
		return revErr
	}
	for k != nil || refK != nil {
		count++
		if count%10_000_000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			fmt.Printf("Compared %d records\n", count)
		}
		if k == nil {
			fmt.Printf("Missing in db: %x [%x]\n", refK, refV)
			refK, refV, revErr = refC.Next()
			if revErr != nil {
				return revErr
			}
		} else if refK == nil {
			fmt.Printf("Missing refDB: %x [%x]\n", k, v)
			k, v, e = c.Next()
			if e != nil {
				return e
			}
		} else {
			switch bytes.Compare(k, refK) {
			case -1:
				fmt.Printf("Missing refDB: %x [%x]\n", k, v)
				k, v, e = c.Next()
				if e != nil {
					return e
				}
			case 1:
				fmt.Printf("Missing in db: %x [%x]\n", refK, refV)
				refK, refV, revErr = refC.Next()
				if revErr != nil {
					return revErr
				}
			case 0:
				if !bytes.Equal(v, refV) {
					fmt.Printf("Different values for %x. db: [%x], refDB: [%x]\n", k, v, refV)
				}
				k, v, e = c.Next()
				if e != nil {
					return e
				}
				refK, refV, revErr = refC.Next()
				if revErr != nil {
					return revErr
				}
			default:
				fmt.Printf("Unexpected result of bytes.Compare: %d\n", bytes.Compare(k, refK))
			}
		}
	}
	return nil
}

func fToMdbx(ctx context.Context, logger log.Logger, to string) error {
	file, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dstOpts := mdbx2.New(kv.ChainDB, logger).Path(to).WriteMap(dbWriteMap)
	dst := dstOpts.MustOpen()
	dstTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return err1
	}
	defer dstTx.Rollback()

	commitEvery := time.NewTicker(5 * time.Second)
	defer commitEvery.Stop()
	fileScanner := bufio.NewScanner(file)
	endData := []byte("DATA=END")
	endHeader := []byte("HEADER=END")

MainLoop:
	for {
		bucket := ""
		for { // header
			if !fileScanner.Scan() {
				break
			}
			kk := fileScanner.Bytes()
			if bytes.Equal(kk, endHeader) {
				break
			}

			parts := strings.Split(string(kk), "=")
			k, v := parts[0], parts[1]
			if k == "database" {
				bucket = v
			}
		}
		err = fileScanner.Err()
		if err != nil {
			panic(err)
		}
		err = fileScanner.Err()
		if err != nil {
			panic(err)
		}
		if bucket == "" {
			panic("bucket not parse")
		}

		c, err := dstTx.RwCursor(bucket)
		if err != nil {
			return err
		}
		defer c.Close()

		for {
			if !fileScanner.Scan() {
				break MainLoop
			}
			k := common.CopyBytes(fileScanner.Bytes())
			if bytes.Equal(k, endData) {
				break
			}
			k = common.FromHex(string(k[1:]))
			if !fileScanner.Scan() {
				break MainLoop
			}
			v := common.CopyBytes(fileScanner.Bytes())
			v = common.FromHex(string(v[1:]))

			if casted, ok := c.(kv.RwCursorDupSort); ok {
				if err = casted.AppendDup(k, v); err != nil {
					panic(err)
				}
			} else {
				if err = c.Append(k, v); err != nil {
					panic(err)
				}
			}
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			case <-commitEvery.C:
				logger.Info("Progress", "bucket", bucket, "key", hex.EncodeToString(k))
			}
		}
		c.Close()
		err = fileScanner.Err()
		if err != nil {
			panic(err)
		}
	}
	err = dstTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func CheckSaltFilesExist(dirs datadir.Dirs) error {
	ok, err := state.CheckSaltFilesExist(dirs)
	if err != nil {
		return err
	}
	if !ok {
		return state.ErrCannotStartWithoutSaltFiles
	}
	return nil
}
