package generate

import (
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"os"
	"os/signal"
	"time"
)

func RegenerateTxLookup(chaindata string) error {
	db, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		return err
	}
	db.DeleteBucket(dbutils.TxLookupPrefix)
	startTime := time.Now()
	ch := make(chan os.Signal, 1)
	quitCh := make(chan struct{})
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		close(quitCh)
	}()

	log.Info("TxLookup generation started", "start time", startTime)
	err = stagedsync.TxLookupTransform(db, nil, quitCh, [][]byte{
		dbutils.HeaderHashKey(4000000),
		dbutils.HeaderHashKey(7000000),
		dbutils.HeaderHashKey(9000000),
	})
	if err != nil {
		return err
	}
	log.Info("TxLookup index is successfully regenerated", "it took", time.Since(startTime))
	return nil
}
