package main

import (
	"context"
	"flag"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

type optionsCfg struct {
	ctx             context.Context
	verkleDb        string
	stateDb         string
	workersCount    uint
	tmpdir          string
	disabledLookups bool
}

func analyseOut(cfg optionsCfg) error {
	db, err := mdbx.Open(cfg.verkleDb, log.Root(), false)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := initDB(tx); err != nil {
		return err
	}
	buckets, err := tx.ListBuckets()
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		size, err := tx.BucketSize(bucket)
		if err != nil {
			return err
		}
		log.Info("Bucket Analysis", "name", bucket, "size", datasize.ByteSize(size).HumanReadable())
	}
	return nil
}

func main() {
	ctx := context.Background()
	mainDb := flag.String("state-chaindata", "chaindata", "path to the chaindata database file")
	verkleDb := flag.String("verkle-chaindata", "out", "path to the output chaindata database file")
	workersCount := flag.Uint("workers", 5, "amount of goroutines")
	tmpdir := flag.String("tmpdir", "/tmp/etl-temp", "amount of goroutines")
	action := flag.String("action", "", "action to execute (hashstate, bucketsizes, verkle)")
	disableLookups := flag.Bool("disable-lookups", false, "disable lookups generation (more compact database)")

	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(3), log.StderrHandler))

	opt := optionsCfg{
		ctx:             ctx,
		stateDb:         *mainDb,
		verkleDb:        *verkleDb,
		workersCount:    *workersCount,
		tmpdir:          *tmpdir,
		disabledLookups: *disableLookups,
	}
	switch *action {
	case "hashstate":
		if err := RegeneratePedersenHashstate(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	case "bucketsizes":
		if err := analyseOut(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	case "verkle":
		if err := GenerateVerkleTree(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	case "incremental":
		if err := IncrementVerkleTree(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	default:
		log.Warn("No valid --action specified, aborting")
	}

}
