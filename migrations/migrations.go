package migrations

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

type Migration struct {
	Name string
	Up   func(db ethdb.Database, history, receipts, txIndex, preImages bool) error
}

func NewMigrator() *Migrator {
	return &Migrator{
		Migrations: migrations,
	}
}

type Migrator struct {
	Migrations []Migration
}

func (m *Migrator) Apply(db ethdb.Database, history, receipts, txIndex, preImages bool) error {
	if len(m.Migrations) == 0 {
		return nil
	}

	lastApplied, err := db.Get(dbutils.DatabaseInfoBucket, dbutils.LastAppliedMigration)
	if errors.Is(err, ethdb.ErrKeyNotFound) {
		return err
	}

	i := len(m.Migrations) - 1
	for ; i >= 0; i-- {
		if m.Migrations[i].Name == string(lastApplied) {
			break
		}
	}

	m.Migrations = m.Migrations[i+1:]
	for _, v := range m.Migrations {
		log.Warn("Apply migration", "name", v.Name)
		err := v.Up(db, history, receipts, txIndex, preImages)
		if err != nil {
			return err
		}
		err = db.Put(dbutils.DatabaseInfoBucket, dbutils.LastAppliedMigration, []byte(v.Name))
		if err != nil {
			return err
		}
		log.Warn("Applied migration", "name", v.Name)
	}
	return nil
}

var migrations = []Migration{}
