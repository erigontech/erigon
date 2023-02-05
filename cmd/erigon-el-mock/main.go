package main

import (
	"net"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/log/v3"
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	lis, err := net.Listen("tcp", "127.0.0.1:8989")
	if err != nil {
		log.Warn("[Exec] could not serve service", "reason", err)
	}
	maxReceiveSize := 500 * datasize.MB

	s := grpc.NewServer(grpc.MaxRecvMsgSize(int(maxReceiveSize)))
	execution.RegisterExecutionServer(s, NewEth1Execution(memdb.New()))
	log.Info("Serving mock Execution layer.")
	if err := s.Serve(lis); err != nil {
		log.Error("failed to serve", "err", err)
	}
}
