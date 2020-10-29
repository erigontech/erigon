package migrations

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/stretchr/testify/require"
)

const RUNS = 10

func commitTestHeaders(n int, db rawdb.DatabaseWriter) {
	for i := 0; i < n; i++ {
		bigI := big.NewInt(int64(i))
		header := types.Header{Number: bigI}
		headerHash := header.Hash().Bytes()
		encodedHeader, _ := rlp.EncodeToBytes(header)
		_ = db.Put(dbutils.HeaderPrefix, append(dbutils.EncodeBlockNumber(uint64(i)), headerHash...), encodedHeader)
		_ = db.Put(dbutils.HeaderPrefix, dbutils.HeaderHashKey(uint64(i)), headerHash)
		_ = rawdb.WriteTd(db, header.Hash(), uint64(i), bigI)
	}
}

func TestRemoveCanonical(t *testing.T) {
	require, db := require.New(t), ethdb.NewMemDatabase()

	err := db.KV().Update(context.Background(), func(tx ethdb.Tx) error {
		return tx.(ethdb.BucketMigrator).CreateBucket(dbutils.BlockReceiptsPrefix)
	})
	require.NoError(err)

	migrator := NewMigrator()
	migrator.Migrations = []Migration{headersRemoveCanonicalMapping}
	commitTestHeaders(RUNS, db)
	err = migrator.Apply(db, "")
	require.NoError(err)

	for i := 0; i < RUNS; i++ {
		bigI := big.NewInt(int64(i))
		have := rawdb.ReadHeaderByNumber(db, uint64(i))
		want := types.Header{Number: bigI}
		if have.Hash() != want.Hash() {
			t.Fatalf("Invalid header. headers differs in hash. have %s, want %s", have.Hash().String(), want.Hash().String())
		}
		haveTd, _ := rawdb.ReadTd(db, have.Hash(), uint64(i))

		if haveTd.Cmp(bigI) != 0 {
			t.Fatalf("Invalid header. headers differs in td. have %d, want %d", haveTd.Int64(), i)
		}
	}
	panic("a")
}
