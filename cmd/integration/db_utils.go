package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var cmdCompareBucket = &cobra.Command{
	Use:   "compare_bucket",
	Short: "compare bucket to the same bucket in '--reference_chaindata'",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if referenceChaindata == "" {
			referenceChaindata = chaindata + "-copy"
		}
		err := compareBucket(ctx, chaindata, referenceChaindata, bucket)
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
}

func compareBucket(ctx context.Context, chaindata string, referenceChaindata string, bucket string) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	refDB := ethdb.MustOpen(referenceChaindata)
	defer refDB.Close()

	tx, err := db.KV().Begin(context.Background(), false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	refTX, err := refDB.KV().Begin(context.Background(), false)
	if err != nil {
		return err
	}
	defer refTX.Rollback()

	count := 0
	c := tx.Bucket([]byte(bucket)).Cursor()
	k, v, e := c.First()
	if e != nil {
		return e
	}
	refC := refTX.Bucket([]byte(bucket)).Cursor()
	refK, refV, revErr := refC.First()
	if revErr != nil {
		return revErr
	}
	for k != nil || refK != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		count++
		if count%100000 == 0 {
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
