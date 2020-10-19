package migrations

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestReceiptCbor(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.BlockReceiptsPrefix)
	})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{receiptsCborEncode}
	err = migrator.Apply(db, "")
	require.NoError(err)
}
