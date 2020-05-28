package generate

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"time"
)

func RegenerateIndex(chaindata string, csBucket []byte) error {
	db, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		return err
	}
	ig := core.NewIndexGenerator(db)

	err = ig.DropIndex(dbutils.StorageHistoryBucket)
	if err != nil {
		return err
	}
	startTime:=time.Now()
	fmt.Println("Index generation started", startTime)
	err = ig.GenerateIndex(0, csBucket)
	if err != nil {
		return err
	}
	fmt.Println("Index is successfully regenerated", "it took", time.Since(startTime))
	return nil
}

/**
account index
merge 24m28.76168323s
fill 18m15.03340184s
walk 5m53.023704948s
wri 5m46.932535463s
Index is successfully regenerated it took 54m24.421808829s

 */