package commands

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/mdbx"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var stateBuckets = []string{
	dbutils.HashedAccountsBucket,
	dbutils.HashedStorageBucket,
	dbutils.ContractCodeBucket,
	dbutils.PlainStateBucket,
	dbutils.PlainAccountChangeSetBucket,
	dbutils.PlainStorageChangeSetBucket,
	dbutils.PlainContractCodeBucket,
	dbutils.IncarnationMapBucket,
	dbutils.CodeBucket,
	dbutils.TrieOfAccountsBucket,
	dbutils.TrieOfStorageBucket,
	dbutils.AccountsHistoryBucket,
	dbutils.StorageHistoryBucket,
	dbutils.TxLookupPrefix,
}

var cmdCompareBucket = &cobra.Command{
	Use:   "compare_bucket",
	Short: "compare bucket to the same bucket in '--chaindata.reference'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		if referenceChaindata == "" {
			referenceChaindata = chaindata + "-copy"
		}
		err := compareBucketBetweenDatabases(ctx, chaindata, referenceChaindata, bucket)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

var cmdCompareStates = &cobra.Command{
	Use:   "compare_states",
	Short: "compare state buckets to buckets in '--chaindata.reference'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		if referenceChaindata == "" {
			referenceChaindata = chaindata + "-copy"
		}
		err := compareStates(ctx, chaindata, referenceChaindata)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

var cmdLmdbToMdbx = &cobra.Command{
	Use:   "lmdb_to_mdbx",
	Short: "copy data from '--chaindata' to '--chaindata.to'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		err := lmdbToMdbx(ctx, chaindata, toChaindata)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

var cmdLmdbToLmdb = &cobra.Command{
	Use:   "lmdb_to_lmdb",
	Short: "copy data from '--chaindata' to '--chaindata.to'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		err := lmdbToLmdb(ctx, chaindata, toChaindata)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

var cmdMdbxToMdbx = &cobra.Command{
	Use:   "mdbx_to_mdbx",
	Short: "copy data from '--chaindata' to '--chaindata.to'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		err := mdbxToMdbx(ctx, chaindata, toChaindata)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

var cmdFToMdbx = &cobra.Command{
	Use:   "f_to_mdbx",
	Short: "copy data from '--chaindata' to '--chaindata.to'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		err := fToMdbx(ctx, toChaindata)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

func init() {
	withChaindata(cmdCompareBucket)
	withReferenceChaindata(cmdCompareBucket)
	withBucket(cmdCompareBucket)

	rootCmd.AddCommand(cmdCompareBucket)

	withChaindata(cmdCompareStates)
	withReferenceChaindata(cmdCompareStates)
	withBucket(cmdCompareStates)

	rootCmd.AddCommand(cmdCompareStates)

	withChaindata(cmdLmdbToMdbx)
	withToChaindata(cmdLmdbToMdbx)
	withBucket(cmdLmdbToMdbx)

	rootCmd.AddCommand(cmdLmdbToMdbx)

	withChaindata(cmdLmdbToLmdb)
	withToChaindata(cmdLmdbToLmdb)
	withBucket(cmdLmdbToLmdb)

	rootCmd.AddCommand(cmdLmdbToLmdb)

	withChaindata(cmdMdbxToMdbx)
	withToChaindata(cmdMdbxToMdbx)
	withBucket(cmdMdbxToMdbx)

	rootCmd.AddCommand(cmdMdbxToMdbx)

	withToChaindata(cmdFToMdbx)
	withFile(cmdFToMdbx)
	withBucket(cmdFToMdbx)

	rootCmd.AddCommand(cmdFToMdbx)
}

func compareStates(ctx context.Context, chaindata string, referenceChaindata string) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	refDB := ethdb.MustOpen(referenceChaindata)
	defer refDB.Close()

	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		if err := refDB.RwKV().View(context.Background(), func(refTX ethdb.Tx) error {
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
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	refDB := ethdb.MustOpen(referenceChaindata)
	defer refDB.Close()

	if err := db.RwKV().View(context.Background(), func(tx ethdb.Tx) error {
		return refDB.RwKV().View(context.Background(), func(refTX ethdb.Tx) error {
			return compareBuckets(ctx, tx, bucket, refTX, bucket)
		})
	}); err != nil {
		return err
	}

	return nil
}

func compareBuckets(ctx context.Context, tx ethdb.Tx, b string, refTx ethdb.Tx, refB string) error {
	count := 0
	c, err := tx.Cursor(b)
	if err != nil {
		return err
	}
	k, v, e := c.First()
	if e != nil {
		return e
	}
	refC, err := refTx.Cursor(refB)
	if err != nil {
		return err
	}
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

func fToMdbx(ctx context.Context, to string) error {
	file, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dst := ethdb.NewMDBX().Path(to).MustOpen()
	dstTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return err1
	}
	defer func() {
		dstTx.Rollback()
	}()

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

		var prevK []byte
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

			if casted, ok := c.(ethdb.RwCursorDupSort); ok {
				if bytes.Equal(k, prevK) {
					if err = casted.AppendDup(k, v); err != nil {
						panic(err)
					}
				} else {
					if err = casted.Append(k, v); err != nil {
						panic(err)
					}
				}
				prevK = k
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
				log.Info("Progress", "bucket", bucket, "key", fmt.Sprintf("%x", k))
			}
		}
		prevK = nil
		err = fileScanner.Err()
		if err != nil {
			panic(err)
		}
	}
	err = dstTx.Commit()
	if err != nil {
		return err
	}
	dstTx, err = dst.BeginRw(ctx)
	if err != nil {
		return err
	}
	err = dstTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func lmdbToMdbx(ctx context.Context, from, to string) error {
	_ = os.RemoveAll(to)
	src := ethdb.NewLMDB().Path(from).Flags(func(flags uint) uint { return (flags | lmdb.Readonly) ^ lmdb.NoReadahead }).MustOpen()
	dst := ethdb.NewMDBX().Path(to).MustOpen()
	return kv2kv(ctx, src, dst)
}

func lmdbToLmdb(ctx context.Context, from, to string) error {
	_ = os.RemoveAll(to)
	src := ethdb.NewLMDB().Path(from).Flags(func(flags uint) uint { return (flags | lmdb.Readonly) ^ lmdb.NoReadahead }).MustOpen()
	dst := ethdb.NewLMDB().Path(to).MustOpen()
	return kv2kv(ctx, src, dst)
}

func mdbxToMdbx(ctx context.Context, from, to string) error {
	_ = os.RemoveAll(to)
	src := ethdb.NewMDBX().Path(from).Flags(func(flags uint) uint { return mdbx.Readonly | mdbx.Accede }).MustOpen()
	dst := ethdb.NewMDBX().Path(to).MustOpen()
	return kv2kv(ctx, src, dst)
}

func kv2kv(ctx context.Context, src, dst ethdb.RwKV) error {
	srcTx, err1 := src.Begin(ctx)
	if err1 != nil {
		return err1
	}
	defer srcTx.Rollback()
	dstTx, err1 := dst.BeginRw(ctx)
	if err1 != nil {
		return err1
	}
	defer func() {
		dstTx.Rollback()
	}()

	commitEvery := time.NewTicker(30 * time.Second)
	defer commitEvery.Stop()

	for name, b := range src.AllBuckets() {
		if b.IsDeprecated {
			continue
		}

		c, err := dstTx.RwCursor(name)
		if err != nil {
			return err
		}
		srcC, err := srcTx.Cursor(name)
		if err != nil {
			return err
		}
		var prevK []byte
		casted, isDupsort := c.(ethdb.RwCursorDupSort)

		for k, v, err := srcC.First(); k != nil; k, v, err = srcC.Next() {
			if err != nil {
				return err
			}

			if isDupsort {
				if bytes.Equal(k, prevK) {
					if err = casted.AppendDup(k, v); err != nil {
						panic(err)
					}
				} else {
					if err = casted.Append(k, v); err != nil {
						panic(err)
					}
				}
				prevK = k
			} else {
				if err = c.Append(k, v); err != nil {
					panic(err)
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-commitEvery.C:
				log.Info("Progress", "bucket", name, "key", fmt.Sprintf("%x", k))
				if err2 := dstTx.Commit(); err2 != nil {
					return err2
				}
				dstTx, err = dst.BeginRw(ctx)
				if err != nil {
					return err
				}
				c, err = dstTx.RwCursor(name)
				if err != nil {
					return err
				}
				casted, isDupsort = c.(ethdb.RwCursorDupSort)
			default:
			}
		}
		prevK = nil

		// migrate bucket sequences to native mdbx implementation
		//currentID, err := srcTx.Sequence(name, 0)
		//if err != nil {
		//	return err
		//}
		//_, err = dstTx.Sequence(name, currentID)
		//if err != nil {
		//	return err
		//}
	}
	err := dstTx.Commit()
	if err != nil {
		return err
	}
	dstTx, err = dst.BeginRw(ctx)
	if err != nil {
		return err
	}
	err = dstTx.Commit()
	if err != nil {
		return err
	}
	srcTx.Rollback()
	log.Info("done")
	return nil
}
