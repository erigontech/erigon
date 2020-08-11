package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

var _ node.Service = &RPCDaemonService{}

// RPCDaemonService is used in js console and console tests
type RPCDaemonService struct {
	api    []rpc.API
	db     *ethdb.ObjectDatabase
	txpool *core.TxPool
}

func (*RPCDaemonService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

func (r *RPCDaemonService) Start(server *p2p.Server) error {
	r.api = commands.GetAPI(r.db.KV(), core.RawFromTxPool(r.txpool), []string{"eth", "debug"})
	return nil
}

func (r *RPCDaemonService) Stop() error {
	return nil
}

func (r *RPCDaemonService) APIs() []rpc.API {
	return r.api
}

func New(db *ethdb.ObjectDatabase, txpool *core.TxPool) *RPCDaemonService {
	return &RPCDaemonService{[]rpc.API{}, db, txpool}
}
