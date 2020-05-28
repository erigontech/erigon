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

	err = ig.DropIndex(dbutils.AccountsHistoryBucket)
	if err != nil {
		return err
	}
	startTime := time.Now()
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


caltime 11m52.681896245s
fill 649580695310
walk 248915314990
wri 265573819339
Index is successfully regenerated it took 27m26.497011965s


caltime 8m47.152730189s
merge 26m52.724080005s
fill 1056139209166
walk 505363152516
wri 549504010353
Index is successfully regenerated it took 35m40.326613534s


*/
