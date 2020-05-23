package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"testing"
)

func TestApplyWithInit(t *testing.T) {
	db := ethdb.NewMemDatabase()
	migrations = []Migration{
		{
			"one",
			func(db ethdb.Database, history, receipts, txIndex, preImages bool) error {
				return nil
			},
		},
		{
			"two",
			func(db ethdb.Database, history, receipts, txIndex, preImages bool) error {
				return nil
			},
		},
	}

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err := migrator.Apply(db, false, false, false, false)
	if err != nil {
		t.Fatal()
	}
	v, err := db.Get(dbutils.DatabaseInfoBucket, dbutils.LastAppliedMigration)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != migrations[1].Name {
		t.Fatal()
	}
}

func TestApplyWithoutInit(t *testing.T) {
	db := ethdb.NewMemDatabase()
	migrations = []Migration{
		{
			"one",
			func(db ethdb.Database, history, receipts, txIndex, preImages bool) error {
				t.Fatal("shouldn't been executed")
				return nil
			},
		},
		{
			"two",
			func(db ethdb.Database, history, receipts, txIndex, preImages bool) error {
				return nil
			},
		},
	}
	err := db.Put(dbutils.DatabaseInfoBucket, dbutils.LastAppliedMigration, []byte(migrations[0].Name))
	if err != nil {
		t.Fatal()
	}

	migrator := NewMigrator()
	migrator.Migrations = migrations
	err = migrator.Apply(db, false, false, false, false)
	if err != nil {
		t.Fatal()
	}
	v, err := db.Get(dbutils.DatabaseInfoBucket, dbutils.LastAppliedMigration)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != migrations[1].Name {
		t.Fatal()
	}
}
