package generate

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"time"
)

func RegenerateIndex(chaindata string, csBucket []byte) error {
	db, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		return err
	}
	ig := core.NewIndexGenerator(db)

	err = ig.DropIndex(dbutils.AccountsHistoryBucket)
	if err != nil {
		return err
	}
	startTime := time.Now()
	log.Info("Index generation started", "start time", startTime)
	err = ig.GenerateIndex(0, csBucket)
	if err != nil {
		return err
	}
	log.Info("Index is successfully regenerated", "it took", time.Since(startTime))
	return nil
}
