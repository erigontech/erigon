package main

import (
	"context"
	"time"

	"github.com/gballet/go-verkle"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

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

	if err := initDB(vTx); err != nil {
		return err
	}

	collector := etl.NewCollector("VerkleTrie", cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize*8))
	defer collector.Close()

	if err := regeneratePedersenAccounts(vTx, tx, cfg, collector); err != nil {
		return err
	}
	if err := regeneratePedersenCode(vTx, tx, cfg, collector); err != nil {
		return err
	}

	if err := regeneratePedersenStorage(vTx, tx, cfg, collector); err != nil {
		return err
	}

	verkleCollector := etl.NewCollector(VerkleTrie, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer verkleCollector.Close()
	// Verkle Tree to be built
	root := verkle.New()
	log.Info("Started Verkle Tree creation")

	logInterval := time.NewTicker(30 * time.Second)
	if err := collector.Load(vTx, VerkleTrie, func(k []byte, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if err := root.InsertOrdered(common.CopyBytes(k), common.CopyBytes(v), func(node verkle.VerkleNode) {
			rootHash := node.ComputeCommitment().Bytes()
			encodedNode, err := node.Serialize()
			if err != nil {
				panic(err)
			}
			if err := verkleCollector.Collect(rootHash[:], encodedNode); err != nil {
				panic(err)
			}
			select {
			case <-logInterval.C:
				log.Info("[Verkle] Assembling Verkle Tree", "key", common.Bytes2Hex(k))
			default:
			}
		}); err != nil {
			return err
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return err
	}

	rootHash := root.ComputeCommitment().Bytes()
	encodedNode, err := root.Serialize()
	if err != nil {
		return err
	}
	if err := verkleCollector.Collect(rootHash[:], encodedNode); err != nil {
		return err
	}
	log.Info("Started Verkle Tree Flushing")
	verkleCollector.Load(vTx, VerkleTrie, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})

	log.Info("Verkle Tree Generation completed", "elapsed", time.Since(start))

	var progress uint64
	if progress, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(vTx, stages.VerkleTrie, progress); err != nil {
		return err
	}
	return vTx.Commit()
}
