package service

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
)

func New(db ethdb.Database, ethereum core.Backend, stack *node.Node) {
	apis := commands.APIList(db, core.NewEthBackend(ethereum), nil, cli.Flags{API: []string{"eth", "debug"}}, nil)

	stack.RegisterAPIs(apis)
}
