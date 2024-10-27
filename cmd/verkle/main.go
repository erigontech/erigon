package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/cl/utils"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"

	"github.com/ledgerwatch/erigon/cmd/verkle/verkletrie"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

type optionsCfg struct {
	ctx             context.Context
	verkleDb        string
	stateDb         string
	workersCount    uint
	tmpdir          string
	disabledLookups bool
}

const DumpSize = uint64(20000000000)

func IncrementVerkleTree(ctx context.Context, cfg optionsCfg, logger log.Logger) error {
	start := time.Now()

	db, err := openDB(ctx, cfg.stateDb, log.Root(), true)
	if err != nil {
		logger.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := openDB(ctx, cfg.verkleDb, log.Root(), false)
	if err != nil {
		logger.Error("Error while opening db transaction", "err", err.Error())
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
	verkleWriter := verkletrie.NewVerkleTreeWriter(vTx, cfg.tmpdir, logger)
	defer verkleWriter.Close()
	if err := verkletrie.IncrementAccount(vTx, tx, uint64(cfg.workersCount), verkleWriter, from, to, cfg.tmpdir); err != nil {
		return err
	}
	if _, err := verkletrie.IncrementStorage(vTx, tx, uint64(cfg.workersCount), verkleWriter, from, to, cfg.tmpdir); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(vTx, stages.VerkleTrie, to); err != nil {
		return err
	}

	logger.Info("Finished", "elapesed", time.Since(start))
	return vTx.Commit()
}

func RegeneratePedersenHashstate(ctx context.Context, cfg optionsCfg, logger log.Logger) error {
	db, err := openDB(ctx, cfg.stateDb, log.Root(), true)
	if err != nil {
		logger.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := openDB(ctx, cfg.stateDb, log.Root(), false)
	if err != nil {
		logger.Error("Error while opening db transaction", "err", err.Error())
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

	verleWriter := verkletrie.NewVerkleTreeWriter(vTx, cfg.tmpdir, logger)

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

func GenerateVerkleTree(ctx context.Context, cfg optionsCfg, logger log.Logger) error {
	start := time.Now()
	db, err := openDB(ctx, cfg.stateDb, log.Root(), true)
	if err != nil {
		logger.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := openDB(ctx, cfg.verkleDb, log.Root(), false)
	if err != nil {
		logger.Error("Error while opening db transaction", "err", err.Error())
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

	verkleWriter := verkletrie.NewVerkleTreeWriter(vTx, cfg.tmpdir, logger)

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
	logger.Info("Started Verkle Tree creation")

	var root libcommon.Hash
	if root, err = verkleWriter.CommitVerkleTreeFromScratch(); err != nil {
		return err
	}

	logger.Info("Verkle Tree Generation completed", "elapsed", time.Since(start), "root", common.Bytes2Hex(root[:]))

	var progress uint64
	if progress, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(vTx, stages.VerkleTrie, progress); err != nil {
		return err
	}
	return vTx.Commit()
}

func analyseOut(ctx context.Context, cfg optionsCfg, logger log.Logger) error {
	db, err := openDB(ctx, cfg.verkleDb, logger, false)
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
		logger.Info("Bucket Analysis", "name", bucket, "size", datasize.ByteSize(size).HumanReadable())
	}
	return nil
}

func dump(ctx context.Context, cfg optionsCfg) error {
	db, err := openDB(ctx, cfg.verkleDb, log.Root(), false)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	logInterval := time.NewTicker(30 * time.Second)
	num := 0
	file, err := os.Create(fmt.Sprintf("dump%d", num))
	if err != nil {
		return err
	}
	currWritten := uint64(0)

	verkleCursor, err := tx.Cursor(kv.VerkleTrie)
	if err != nil {
		return err
	}
	for k, v, err := verkleCursor.First(); k != nil; k, v, err = verkleCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) != 32 {
			continue
		}
		// k is the root so it will always be 32 bytes
		if _, err := file.Write(k); err != nil {
			return err
		}
		currWritten += 32
		// Write length of RLP encoded note
		lenNode := make([]byte, 8)
		binary.BigEndian.PutUint64(lenNode, uint64(len(v)))
		if _, err := file.Write(lenNode); err != nil {
			return err
		}
		currWritten += 8
		// Write Rlp encoded node
		if _, err := file.Write(v); err != nil {
			return err
		}
		currWritten += uint64(len(v))
		if currWritten > DumpSize {
			file.Close()
			currWritten = 0
			num++
			file, err = os.Create(fmt.Sprintf("dump%d", num))
			if err != nil {
				return err
			}
		}
		select {
		case <-logInterval.C:
			log.Info("Dumping verkle tree to plain text", "key", common.Bytes2Hex(k))
		default:
		}
	}
	file.Close()
	return nil
}

func dump_acc_preimages(ctx context.Context, cfg optionsCfg) error {
	db, err := openDB(ctx, cfg.stateDb, log.Root(), false)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	logInterval := time.NewTicker(30 * time.Second)
	file, err := os.Create("dump_accounts")
	if err != nil {
		return err
	}

	stateCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	num, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("Current Block Number", "num", num)
	for k, _, err := stateCursor.First(); k != nil; k, _, err = stateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) != 20 {
			continue
		}
		// Address
		if _, err := file.Write(k); err != nil {
			return err
		}
		addressHash := utils.Sha256(k)

		if _, err := file.Write(addressHash[:]); err != nil {
			return err
		}

		select {
		case <-logInterval.C:
			log.Info("Dumping preimages to plain text", "key", common.Bytes2Hex(k))
		default:
		}
	}
	file.Close()
	return nil
}

func dump_storage_preimages(ctx context.Context, cfg optionsCfg, logger log.Logger) error {
	db, err := openDB(ctx, cfg.stateDb, logger, false)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logInterval := time.NewTicker(30 * time.Second)
	file, err := os.Create("dump_storage")
	if err != nil {
		return err
	}
	collector := etl.NewCollector(".", "/tmp", etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer collector.Close()
	stateCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	num, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	logger.Info("Current Block Number", "num", num)
	var currentIncarnation uint64
	var currentAddress libcommon.Address
	var addressHash libcommon.Hash
	var buf buffer.Buffer
	for k, v, err := stateCursor.First(); k != nil; k, v, err = stateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) == 20 {
			if currentAddress != (libcommon.Address{}) {
				if err := collector.Collect(addressHash[:], buf.Bytes()); err != nil {
					return err
				}
			}
			buf.Reset()
			var acc accounts.Account
			if err := acc.DecodeForStorage(v); err != nil {
				return err
			}
			currentAddress = libcommon.BytesToAddress(k)
			currentIncarnation = acc.Incarnation
			addressHash = utils.Sha256(currentAddress[:])
		} else {
			address := libcommon.BytesToAddress(k[:20])
			if address != currentAddress {
				continue
			}
			if binary.BigEndian.Uint64(k[20:]) != currentIncarnation {
				continue
			}
			storageHash := utils.Sha256(k[28:])
			buf.Write(k[28:])
			buf.Write(storageHash[:])
		}

		select {
		case <-logInterval.C:
			logger.Info("Computing preimages to plain text", "key", common.Bytes2Hex(k))
		default:
		}
	}
	if err := collector.Collect(addressHash[:], buf.Bytes()); err != nil {
		return err
	}
	collector.Load(tx, "", func(k, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		_, err := file.Write(v)
		return err
	}, etl.TransformArgs{})
	file.Close()
	return nil
}

func dump_pre_images(ctx context.Context, cfg optionsCfg, logger log.Logger) error {
	db, err := openDB(ctx, cfg.stateDb, logger, false)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logInterval := time.NewTicker(120 * time.Second)
	accFile, err := os.Create("acc_pre_images.dat")
	accHashKeysFile, err := os.Create("acc_hashed_keys.dat")
	storageFile, err := os.Create("storage_pre_images.dat")
	storageHashKeysFile, err := os.Create("storage_hashed_keys.dat")
	if err != nil {
		return err
	}
	// collector := etl.NewCollector(".", "/tmp", etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	accCollector := etl.NewCollector(".", cfg.tmpdir+"/tmpAcc", etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	storageCollector := etl.NewCollector(".", cfg.tmpdir+"/tmpStg", etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer accCollector.Close()
	defer storageCollector.Close()
	stateCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	num, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	logger.Info("Current Block Number", "num", num)
	var currentIncarnation uint64
	var currentAddress libcommon.Address
	var addressHash libcommon.Hash
	var keyCounter, accCounter, storageCounter uint64
	var notCurrAddrCounter = 0
	var notCurrIncarnation = 0
	acc := accounts.Account{}

	// var buf buffer.Buffer
	for k, v, err := stateCursor.First(); k != nil; k, _, err = stateCursor.Next() {
		if err != nil {
			return err
		}
		keyCounter++
		if len(k) == 20 {
			currentAddress = libcommon.BytesToAddress(k)
			addressHash = utils.Sha256(currentAddress[:])
			if err := acc.DecodeForStorage(v); err != nil {
				return err
			}

			// Storage will have incarnation 1 by default, acc incarnation will be incremented on SelfDestruct
			currentIncarnation = acc.Incarnation + 1

			if err := accCollector.Collect(addressHash[:], k); err != nil {
				return err
			}
			accCounter++
		} else {
			// logger.Info("Iterating through storage key", "k", k, "k[28:]", k[28:])

			address := libcommon.BytesToAddress(k[:20])
			if address != currentAddress {
				notCurrAddrCounter++
				continue
			}
			inc := binary.BigEndian.Uint64(k[20:])
			if inc != currentIncarnation {
				notCurrIncarnation++
				continue
			}
			storageHash := utils.Sha256(k[28:])
			if err := storageCollector.Collect(storageHash[:], k[28:]); err != nil {
				return err
			}
			// buf.Write(k[28:])
			// buf.Write(storageHash[:])
			storageCounter++
		}

		select {
		case <-logInterval.C:
			logger.Info("Computing preimages to state key", "key", common.Bytes2Hex(k), "accCounter", accCounter, "storageCounter", storageCounter)
		default:
		}
	}
	// if err := collector.Collect(addressHash[:], buf.Bytes()); err != nil {
	// 	return err
	// }
	// db2, err := openDB(ctx, cfg.stateDb, logger, false)
	logger.Info("Beginning writing file ", "keyCounter", keyCounter, "accCounter", accCounter, "storageCounter", storageCounter)

	accCounter = 0
	storageCounter = 0
	accCollector.Load(tx, "", func(k, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		_, err := accFile.Write(v)
		if err != nil {
			return err
		}
		_, err = accHashKeysFile.Write(k)
		if err != nil { 
			return err
		}
		accCounter++
		select {
		case <-logInterval.C:
			logger.Info("Computing preimages to state key", "acc", common.Bytes2Hex(k), "accCounter", accCounter)
		default:
		}
		return err
	}, etl.TransformArgs{})

	storageCollector.Load(tx, "", func(k, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		_, err := storageFile.Write(v)
		if err != nil {
			return err
		}
		_, err = storageHashKeysFile.Write(k)
		if err != nil {
			return err
		}
		storageCounter++
		select {
		case <-logInterval.C:
			logger.Info("Computing preimages to state key", "key", common.Bytes2Hex(k), "storageCounter", storageCounter)
		default:
		}
		return err
	}, etl.TransformArgs{})

	logger.Info("Finished writing file ", "keyCounter", keyCounter, "accCounter", accCounter, "storageCounter", storageCounter, "notCurrIncarnation", notCurrIncarnation, "notCurrAddrCounter", notCurrAddrCounter)
	return nil
}

func main() {
	ctx := context.Background()
	// mainDb := flag.String("state-chaindata", "chaindata", "path to the chaindata database file")
	mainDb := flag.String("datadir", "chaindata", "path to the chaindata database file")
	verkleDb := flag.String("verkle-chaindata", "out", "path to the output chaindata database file")
	workersCount := flag.Uint("workers", 5, "amount of goroutines")
	tmpdir := flag.String("tmpdir", "/tmp/etl-temp", "amount of goroutines")
	action := flag.String("action", "", "action to execute (hashstate, bucketsizes, verkle)")
	disableLookups := flag.Bool("disable-lookups", false, "disable lookups generation (more compact database)")

	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(3), log.StderrHandler))
	logger := log.Root()

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
		if err := RegeneratePedersenHashstate(ctx, opt, logger); err != nil {
			logger.Error("Error", "err", err.Error())
		}
	case "bucketsizes":
		if err := analyseOut(ctx, opt, logger); err != nil {
			logger.Error("Error", "err", err.Error())
		}
	case "verkle":
		if err := GenerateVerkleTree(ctx, opt, logger); err != nil {
			logger.Error("Error", "err", err.Error())
		}
	case "incremental":
		if err := IncrementVerkleTree(ctx, opt, logger); err != nil {
			logger.Error("Error", "err", err.Error())
		}
	case "dump_preimages":
		if err := dump_pre_images(ctx, opt, logger); err != nil {
			logger.Error("Error", "err", err.Error())
		}
	default:
		log.Warn("No valid --action specified, aborting")
	}
}

func openDB(ctx context.Context, path string, logger log.Logger, accede bool) (kv.RwDB, error) {
	var db kv.RwDB
	var err error
	opts := mdbx.NewMDBX(logger).Path(path)
	if accede {
		opts = opts.Accede()
	}
	db, err = opts.Open(ctx)

	if err != nil {
		return nil, err
	}
	return db, nil
}
