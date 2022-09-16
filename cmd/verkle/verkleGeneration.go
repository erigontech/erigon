package main

import (
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

func int256ToVerkleFormat(x *uint256.Int, buffer []byte) {
	bbytes := x.ToBig().Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			buffer[len(bbytes)-i-1] = b
		}
	}
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

	if err := initDB(vTx); err != nil {
		return err
	}

	verkleWriter := NewVerkleTreeWriter(vTx, cfg.tmpdir)

	if err := regeneratePedersenAccounts(vTx, tx, cfg, verkleWriter); err != nil {
		return err
	}
	if err := regeneratePedersenCode(vTx, tx, cfg, verkleWriter); err != nil {
		return err
	}

	if err := regeneratePedersenStorage(vTx, tx, cfg, verkleWriter); err != nil {
		return err
	}

	verkleCollector := etl.NewCollector(VerkleTrie, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer verkleCollector.Close()
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
