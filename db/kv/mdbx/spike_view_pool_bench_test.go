package mdbx_test

import (
	"testing"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon/db/kv/mdbx"
)

func spikeEnv(b *testing.B) (*mdbx.MdbxKV, *mdbxgo.Env) {
	db := BaseCaseDBForBenchmark(b).(*mdbx.MdbxKV)
	return db, db.Env()
}

func BenchmarkSpikeErigonBeginRoRollback(b *testing.B) {
	db, _ := spikeEnv(b)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := db.BeginRo(b.Context()) //nolint:gocritic
			if err != nil {
				b.Error(err)
				return
			}
			tx.Rollback()
		}
	})
}

func BenchmarkSpikeResetRenew(b *testing.B) {
	_, env := spikeEnv(b)
	b.RunParallel(func(pb *testing.PB) {
		txn, err := env.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			b.Error(err)
			return
		}
		defer txn.Abort()
		for pb.Next() {
			txn.Reset()
			if err := txn.Renew(); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSpikeParkUnpark(b *testing.B) {
	_, env := spikeEnv(b)
	b.RunParallel(func(pb *testing.PB) {
		txn, err := env.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			b.Error(err)
			return
		}
		defer txn.Abort()
		for pb.Next() {
			if err := txn.Park(false); err != nil {
				b.Error(err)
				return
			}
			if _, err := txn.Unpark(false); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSpikeEnvInfo(b *testing.B) {
	_, env := spikeEnv(b)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := env.Info(nil); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
