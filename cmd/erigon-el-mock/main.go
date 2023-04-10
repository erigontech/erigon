package main

import (
	"flag"
	"net"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/log/v3"
)

func main() {

	datadir := flag.String("datadir", "", "non in-memory db for EL simulation")
	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	lis, err := net.Listen("tcp", "127.0.0.1:8989")
	if err != nil {
		log.Warn("[Exec] could not serve service", "reason", err)
	}
	maxReceiveSize := 500 * datasize.MB

	s := grpc.NewServer(grpc.MaxRecvMsgSize(int(maxReceiveSize)))
	var db kv.RwDB
	if *datadir == "" {
		db = memdb.New("")
	} else {
		db, err = mdbx.Open(*datadir, log.Root(), false)
		if err != nil {
			log.Error("Could not open database", "err", err)
			return
		}
	}
	execution.RegisterExecutionServer(s, NewEth1Execution(db))
	log.Info("Serving mock Execution layer.")
	if err := s.Serve(lis); err != nil {
		log.Error("failed to serve", "err", err)
	}
}
