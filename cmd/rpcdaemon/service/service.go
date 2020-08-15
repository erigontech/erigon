package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
)

func New(db ethdb.HasKV, ethereum *eth.Ethereum, stack *node.Node) {
	apis := commands.GetAPI(db.KV(), core.NewEthBackend(ethereum), []string{"eth", "debug"})

	stack.RegisterAPIs(apis)
}
