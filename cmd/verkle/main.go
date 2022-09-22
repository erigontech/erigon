package main

import (
	"context"
	"flag"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/verkle/verkletrie"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
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

func IncrementVerkleTree(cfg optionsCfg) error {
	start := time.Now()

	db, err := mdbx.Open(cfg.stateDb, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := mdbx.Open(cfg.verkleDb, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return err
	}
	defer vDb.Close()

	vTx, err := vDb.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer vTx.Rollback()

	tx, err := db.BeginRo(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	from, err := stages.GetStageProgress(vTx, stages.VerkleTrie)
	if err != nil {
		return err
	}

	to, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	verkleWriter := verkletrie.NewVerkleTreeWriter(vTx, cfg.tmpdir)
	defer verkleWriter.Close()
	if err := verkletrie.IncrementAccount(vTx, tx, uint64(cfg.workersCount), verkleWriter, from, to); err != nil {
		return err
	}
	if _, err := verkletrie.IncrementStorage(vTx, tx, uint64(cfg.workersCount), verkleWriter, from, to); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(vTx, stages.VerkleTrie, to); err != nil {
		return err
	}

	log.Info("Finished", "elapesed", time.Since(start))
	return vTx.Commit()
}

func RegeneratePedersenHashstate(cfg optionsCfg) error {
	db, err := mdbx.Open(cfg.stateDb, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := mdbx.Open(cfg.stateDb, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return err
	}
	defer vDb.Close()

	vTx, err := vDb.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer vTx.Rollback()

	tx, err := db.BeginRo(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	verleWriter := verkletrie.NewVerkleTreeWriter(vTx, cfg.tmpdir)

	if err := verkletrie.RegeneratePedersenAccounts(vTx, tx, uint64(cfg.workersCount), verleWriter); err != nil {
		return err
	}
	if err := verkletrie.RegeneratePedersenCode(vTx, tx, uint64(cfg.workersCount), verleWriter); err != nil {
		return err
	}

	if err := verkletrie.RegeneratePedersenStorage(vTx, tx, uint64(cfg.workersCount), verleWriter); err != nil {
		return err
	}
	return vTx.Commit()
}

func GenerateVerkleTree(cfg optionsCfg) error {
	start := time.Now()
	db, err := mdbx.Open(cfg.stateDb, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := mdbx.Open(cfg.verkleDb, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return err
	}
	defer vDb.Close()

	vTx, err := vDb.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer vTx.Rollback()

	tx, err := db.BeginRo(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	verkleWriter := verkletrie.NewVerkleTreeWriter(vTx, cfg.tmpdir)

	if err := verkletrie.RegeneratePedersenAccounts(vTx, tx, uint64(cfg.workersCount), verkleWriter); err != nil {
		return err
	}
	if err := verkletrie.RegeneratePedersenCode(vTx, tx, uint64(cfg.workersCount), verkleWriter); err != nil {
		return err
	}

	if err := verkletrie.RegeneratePedersenStorage(vTx, tx, uint64(cfg.workersCount), verkleWriter); err != nil {
		return err
	}

	// Verkle Tree to be built
	log.Info("Started Verkle Tree creation")

	var root common.Hash
	if root, err = verkleWriter.CommitVerkleTreeFromScratch(); err != nil {
		return err
	}

	log.Info("Verkle Tree Generation completed", "elapsed", time.Since(start), "root", common.Bytes2Hex(root[:]))

	var progress uint64
	if progress, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(vTx, stages.VerkleTrie, progress); err != nil {
		return err
	}
	return vTx.Commit()
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
