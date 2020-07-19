package commands

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var stateBuckets = [][]byte{
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
		ctx := rootContext()
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
		ctx := rootContext()
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

func init() {
	withChaindata(cmdCompareBucket)
	withReferenceChaindata(cmdCompareBucket)
	withBucket(cmdCompareBucket)

	rootCmd.AddCommand(cmdCompareBucket)

	withChaindata(cmdCompareStates)
	withReferenceChaindata(cmdCompareStates)
	withBucket(cmdCompareStates)

	rootCmd.AddCommand(cmdCompareStates)
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
				if err := compareBuckets(ctx, tx.Bucket(bucket), refTX.Bucket(bucket)); err != nil {
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
			return compareBuckets(ctx, tx.Bucket([]byte(bucket)), refTX.Bucket([]byte(bucket)))
		})
	}); err != nil {
		return err
	}

	return nil
}

func compareBuckets(ctx context.Context, b ethdb.Bucket, refB ethdb.Bucket) error {
	count := 0
	c := b.Cursor()
	k, v, e := c.First()
	if e != nil {
		return e
	}
	refC := refB.Cursor()
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
