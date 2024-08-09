package utils

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func PopulateMemoryMutationTables(batch kv.RwTx) error {
	for _, table := range hermez_db.HermezDbTables {
		if err := batch.CreateBucket(table); err != nil {
			return err
		}
	}

	for _, table := range db.HermezSmtTables {
		if err := batch.CreateBucket(table); err != nil {
			return err
		}
	}

	for _, table := range kv.ChaindataTables {
		if err := batch.CreateBucket(table); err != nil {
			return err
		}
	}

	return nil
}
