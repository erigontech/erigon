package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

var _ node.Service = &RPCDaemonService{}

func (*RPCDaemonService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

func (r *RPCDaemonService) Start(server *p2p.Server) error {
	var rpcAPI = []rpc.API{}

	db := r.db.KV()
	dbReader := ethdb.NewObjectDatabase(db)
	chainContext := commands.NewChainContext(dbReader)
	apiImpl := commands.NewAPI(db, dbReader, chainContext)
	dbgAPIImpl := commands.NewPrivateDebugAPI(db, dbReader, chainContext)

	rpcAPI = append(rpcAPI, rpc.API{
		Namespace: "eth",
		Public:    true,
		Service:   commands.EthAPI(apiImpl),
		Version:   "1.0",
	})
	rpcAPI = append(rpcAPI, rpc.API{
		Namespace: "debug",
		Public:    true,
		Service:   commands.PrivateDebugAPI(dbgAPIImpl),
		Version:   "1.0",
	})

	r.api = rpcAPI

	return nil
}

func (r *RPCDaemonService) Stop() error {
	return nil
}

func (r *RPCDaemonService) APIs() []rpc.API {
	return r.api
}

type RPCDaemonService struct {
	api []rpc.API
	db  *ethdb.ObjectDatabase
}

func New(db *ethdb.ObjectDatabase) *RPCDaemonService {
	return &RPCDaemonService{[]rpc.API{}, db}
}
