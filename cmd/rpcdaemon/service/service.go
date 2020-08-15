package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// RPCDaemonService is used in js console and console tests
type RPCDaemonService struct {
	api    []rpc.API
	db     *ethdb.ObjectDatabase
	txpool *core.TxPool
}

func New(db *ethdb.ObjectDatabase, txpool *core.TxPool, stack *node.Node) *RPCDaemonService {
	service := &RPCDaemonService{[]rpc.API{}, db, txpool}
	apis := commands.GetAPI(db.KV(), core.RawFromTxPool(txpool), []string{"eth", "debug"})

	stack.RegisterAPIs(apis)

	return service
}
