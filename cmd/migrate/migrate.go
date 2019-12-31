package main

import (
	"flag"
	"fmt"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

func encodeToCBOR(buf []byte) ([]byte, error) {
	acc, err := decodeRLP(buf)

	if err != nil {
		return nil, err
	}

	res := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(res)
	return res, nil
}

// This method does not Commit
func convertDatabaseToCBOR(db *bolt.DB, maxTxOperations uint) error {
	var k, v []byte
	var done bool = false
	for !done {
		var i uint
		err := db.Update(func(tx *bolt.Tx) error {
			var accountBucket *bolt.Bucket = tx.Bucket(dbutils.AccountsBucket)
			c := accountBucket.Cursor()

			if k == nil {
				k, v = c.First()
			} else {
				k, v = c.Seek(k)
				k, v = c.Next()
			}

			for ; k != nil; k, v = c.Next() {

				enc, err := encodeToCBOR(v)
				if err != nil {
					return err
				}
				i++
				err = accountBucket.Put(k, enc)
				if err != nil {
					return err
				}
				if i+1 == maxTxOperations {
					break
				}
			}

			if k == nil {
				done = true
			}
			k = common.CopyBytes(k)
			return nil
		})

		if err != nil {
			return err
		}
		i = 0
	}
	return nil
}

func main() {
	path := flag.String("path", "default", "path to database")
	maxTxOperations := flag.Uint("number_operations", 100000, "maximun amount of Put operations before commiting the transaction to the database")
	flag.Parse()

	if *path == "default" {
		fmt.Println("-path must be specified")
		return
	}

	db, err := bolt.Open(*path, 0600, &bolt.Options{})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = convertDatabaseToCBOR(db, *maxTxOperations)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}
