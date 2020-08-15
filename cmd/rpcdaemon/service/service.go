package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

var _ node.Lifecycle = &RPCDaemonService{}

// RPCDaemonService is used in js console and console tests
type RPCDaemonService struct {
	api    []rpc.API
	db     *ethdb.ObjectDatabase
	txpool *core.TxPool
}

func (r *RPCDaemonService) Start() error {
	return nil
}

func (r *RPCDaemonService) Stop() error {
	return nil
}

func New(db *ethdb.ObjectDatabase, txpool *core.TxPool, stack *node.Node) *RPCDaemonService {
	service := &RPCDaemonService{[]rpc.API{}, db, txpool}
	apis := commands.GetAPI(db.KV(), core.RawFromTxPool(txpool), []string{"eth", "debug"})

	stack.RegisterLifecycle(service)
	stack.RegisterAPIs(apis)

	return service
}
