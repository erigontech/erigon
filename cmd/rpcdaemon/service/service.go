package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
)

func New(db ethdb.HasKV, ethereum core.Backend, stack *node.Node) {
	apis := commands.APIList(db.KV(), core.NewEthBackend(ethereum), []string{"eth", "debug"}, 0)

	stack.RegisterAPIs(apis)
}
