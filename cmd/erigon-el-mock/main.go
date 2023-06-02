package main

import (
	"context"
	"flag"
	"net"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/log/v3"
)

func main() {
	datadirPtr := flag.String("datadir2", "", "non in-memory db for EL simulation")
	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	lis, err := net.Listen("tcp", "127.0.0.1:8989")
	if err != nil {
		log.Warn("[Exec] could not serve service", "reason", err)
	}
	maxReceiveSize := 500 * datasize.MB
	dirs := datadir.New(*datadirPtr)

	s := grpc.NewServer(grpc.MaxRecvMsgSize(int(maxReceiveSize)))
	var db kv.RwDB
	if *datadirPtr == "" {
		db = memdb.New("")
	} else {
		db, err = mdbx.Open(dirs.DataDir, log.Root(), false)
		if err != nil {
			log.Error("Could not open database", "err", err)
			return
		}
	}
	blockReader, blockWriter := blocksIO(db)
	execution.RegisterExecutionServer(s, NewEth1Execution(db, blockReader, blockWriter))
	log.Info("Serving mock Execution layer.")
	if err := s.Serve(lis); err != nil {
		log.Error("failed to serve", "err", err)
	}
}

func blocksIO(db kv.RoDB) (services.FullBlockReader, *blockio.BlockWriter) {
	var histV3, transactionsV3 bool
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		transactionsV3, _ = kvcfg.TransactionsV3.Enabled(tx)
		histV3, _ = kvcfg.HistoryV3.Enabled(tx)
		return nil
	}); err != nil {
		panic(err)
	}
	br := snapshotsync.NewBlockReader(snapshotsync.NewRoSnapshots(ethconfig.Snapshot{Enabled: false}, "", log.New()), transactionsV3)
	bw := blockio.NewBlockWriter(histV3, transactionsV3)
	return br, bw
}
