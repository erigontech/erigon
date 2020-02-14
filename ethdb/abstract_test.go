package ethdb_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestExample1(t *testing.T) {
	ctx := context.Background()

	//ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Badger).InMemory(true))
	db, err := ethdb.Open(ctx, ethdb.Opts().InMemory(true))
	if err != nil {
		panic(err)
	}

	if err := db.View(ctx, func(tx *ethdb.Tx) error {
		b, err := tx.Bucket([]byte("alex"))
		if err != nil {
			return err
		}

		c, err := b.Cursor(b.CursorOpts().PrefetchSize(1000))
		if err != nil {
			return err
		}
		_ = c
		return nil
	}); err != nil {
		panic(err)
	}
}
