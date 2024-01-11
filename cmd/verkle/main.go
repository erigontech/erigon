package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"

	"github.com/ledgerwatch/erigon/cl/utils"
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

	var root libcommon.Hash
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

func dump(cfg optionsCfg) error {
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

func dump_acc_preimages(cfg optionsCfg) error {
	db, err := mdbx.Open(cfg.stateDb, log.Root(), false)
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
		addressHash := utils.Keccak256(k)

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

func dump_storage_preimages(cfg optionsCfg) error {
	db, err := mdbx.Open(cfg.stateDb, log.Root(), false)
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
	collector := etl.NewCollector(".", "/tmp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close()
	stateCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	num, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	log.Info("Current Block Number", "num", num)
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
			addressHash = utils.Keccak256(currentAddress[:])
		} else {
			address := libcommon.BytesToAddress(k[:20])
			if address != currentAddress {
				continue
			}
			if binary.BigEndian.Uint64(k[20:]) != currentIncarnation {
				continue
			}
			storageHash := utils.Keccak256(k[28:])
			buf.Write(k[28:])
			buf.Write(storageHash[:])
		}

		select {
		case <-logInterval.C:
			log.Info("Computing preimages to plain text", "key", common.Bytes2Hex(k))
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
	case "dump":
		log.Info("Dumping in dump.txt")
		if err := dump(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	case "acc_preimages":
		if err := dump_acc_preimages(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	case "storage_preimages":
		if err := dump_storage_preimages(opt); err != nil {
			log.Error("Error", "err", err.Error())
		}
	default:
		log.Warn("No valid --action specified, aborting")
	}
}
