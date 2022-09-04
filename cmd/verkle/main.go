package main

import (
	"context"
	"flag"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

func analyseOut(tx kv.RwTx) error {
	buckets, err := tx.ListBuckets()
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		size, err := tx.BucketSize(bucket)
		if err != nil {
			return err
		}

		log.Info("Bucket Analysis", "name", bucket, "size", size)
	}
	return nil
}

func main() {
	ctx := context.Background()
	chaindata := flag.String("chaindata", "chaindata", "path to the chaindata database file")
	out := flag.String("out", "out", "path to the output chaindata database file")
	workersCount := flag.Uint("workers", 5, "amount of goroutines")
	action := flag.String("action", "", "action to execute")

	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(3), log.StderrHandler))
	db, err := mdbx.Open(*chaindata, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return
	}
	defer db.Close()

	dbOut, err := mdbx.Open(*out, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	defer dbOut.Close()

	txOut, err := dbOut.BeginRw(ctx)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	defer txOut.Rollback()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	defer tx.Rollback()

	log.Info("Opened Database", "datadir", *chaindata)
	switch *action {
	case "PedersenHashState":
		if err := RegeneratePedersenHashstate(txOut, tx, *workersCount); err != nil {
			log.Error("Error", "err", err.Error())
		}
	case "analyseOut":
		if err := analyseOut(txOut); err != nil {
			log.Error("Error", "err", err.Error())
		}
	}

}
