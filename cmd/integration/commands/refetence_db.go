package commands

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var stateBuckets = []string{
	dbutils.CurrentStateBucket,
	dbutils.AccountChangeSetBucket,
	dbutils.StorageChangeSetBucket,
	dbutils.ContractCodeBucket,
	dbutils.PlainStateBucket,
	dbutils.PlainAccountChangeSetBucket,
	dbutils.PlainStorageChangeSetBucket,
	dbutils.PlainContractCodeBucket,
	dbutils.IncarnationMapBucket,
	dbutils.CodeBucket,
	dbutils.IntermediateTrieHashBucket,
	dbutils.AccountsHistoryBucket,
	dbutils.StorageHistoryBucket,
	dbutils.TxLookupPrefix,
}

var cmdCompareBucket = &cobra.Command{
	Use:   "compare_bucket",
	Short: "compare bucket to the same bucket in '--reference_chaindata'",
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
	Short: "compare state buckets to buckets in '--reference_chaindata'",
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

var cmdToMdbx = &cobra.Command{
	Use:   "to_mdbx",
	Short: "copy data from '--chaindata' to '--reference_chaindata'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		err := toMdbx(ctx, chaindata, toChaindata)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		return nil
	},
}

var cmdFToMdbx = &cobra.Command{
	Use:   "f_to_mdbx",
	Short: "copy data from '--chaindata' to '--reference_chaindata'",
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

	withChaindata(cmdToMdbx)
	withToChaindata(cmdToMdbx)
	withBucket(cmdToMdbx)

	rootCmd.AddCommand(cmdToMdbx)

	withToChaindata(cmdFToMdbx)
	withBucket(cmdFToMdbx)

	rootCmd.AddCommand(cmdFToMdbx)
}

func compareStates(ctx context.Context, chaindata string, referenceChaindata string) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	refDB := ethdb.MustOpen(referenceChaindata)
	defer refDB.Close()

	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		if err := refDB.KV().View(context.Background(), func(refTX ethdb.Tx) error {
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

	if err := db.KV().View(context.Background(), func(tx ethdb.Tx) error {
		return refDB.KV().View(context.Background(), func(refTX ethdb.Tx) error {
			return compareBuckets(ctx, tx, bucket, refTX, bucket)
		})
	}); err != nil {
		return err
	}

	return nil
}

func compareBuckets(ctx context.Context, tx ethdb.Tx, b string, refTx ethdb.Tx, refB string) error {
	count := 0
	c := tx.Cursor(b)
	k, v, e := c.First()
	if e != nil {
		return e
	}
	refC := refTx.Cursor(refB)
	refK, refV, revErr := refC.First()
	if revErr != nil {
		return revErr
	}
	for k != nil || refK != nil {
		count++
		if count%100_000 == 0 {
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
	file, err := os.Open("/media/alex/evo/alex_full.log")
	if err != nil {
		fmt.Printf("Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	dst := ethdb.NewMDBX().Path(to).MustOpen()
	dstTx, err1 := dst.Begin(ctx, nil, true)
	if err1 != nil {
		return err1
	}
	defer func() {
		dstTx.Rollback()
	}()

	logEvery := time.NewTicker(15 * time.Second)
	defer logEvery.Stop()

	commitEvery := time.NewTicker(5 * time.Minute)
	defer commitEvery.Stop()

	fileScanner := bufio.NewScanner(file)
	c := dstTx.CursorDupSort(dbutils.CurrentStateBucket3)
	i := 0
	for fileScanner.Scan() {
		i++
		kv := strings.Split(fileScanner.Text(), ",")
		k, _ := hex.DecodeString(kv[0])
		v, _ := hex.DecodeString(kv[1])
		if k[0] < uint8(151) {
			continue
		}
		if err = c.AppendDup(k, v); err != nil {
			return err
		}

		select {
		default:
		case <-logEvery.C:
			log.Info("Progress", "key", fmt.Sprintf("%x", k))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := fileScanner.Err(); err != nil {
		fmt.Println(err)
	}
	err = dstTx.Commit(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func toMdbx(ctx context.Context, from, to string) error {
	_ = os.RemoveAll(to)

	src := ethdb.NewLMDB().Path(from).MustOpen()
	dst := ethdb.NewMDBX().Path(to).MustOpen()
	srcTx, err1 := src.Begin(ctx, nil, false)
	if err1 != nil {
		return err1
	}
	defer srcTx.Rollback()
	dstTx, err1 := dst.Begin(ctx, nil, true)
	if err1 != nil {
		return err1
	}
	defer func() {
		dstTx.Rollback()
	}()

	logEvery := time.NewTicker(15 * time.Second)
	defer logEvery.Stop()

	commitEvery := time.NewTicker(5 * time.Minute)
	defer commitEvery.Stop()

	for name, b := range dbutils.BucketsConfigs {
		if b.IsDeprecated {
			continue
		}
		if name != dbutils.CurrentStateBucket {
			continue
		}

		c := dstTx.Cursor(name)
		appendFunc := c.Append
		if b.Flags&dbutils.DupSort != 0 && !b.AutoDupSortKeysConversion {
			appendFunc = c.(ethdb.CursorDupSort).AppendDup
		}

		srcC := srcTx.Cursor(name)
		for k, v, err := srcC.First(); k != nil; k, v, err = srcC.Next() {
			if err != nil {
				return err
			}

			if err = appendFunc(k, v); err != nil {
				return err
			}

			select {
			default:
			case <-logEvery.C:
				log.Info("Progress", "bucket", name, "key", fmt.Sprintf("%x", k))
			case <-commitEvery.C:
				if err2 := dstTx.Commit(ctx); err2 != nil {
					return err2
				}
				dstTx, err = dst.Begin(ctx, nil, true)
				if err != nil {
					return err
				}
				c = dstTx.Cursor(name)
				appendFunc = c.Append
				if b.Flags&dbutils.DupSort != 0 && !b.AutoDupSortKeysConversion {
					appendFunc = c.(ethdb.CursorDupSort).AppendDup
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	err := dstTx.Commit(context.Background())
	if err != nil {
		return err
	}
	srcTx.Rollback()
	return nil
}
