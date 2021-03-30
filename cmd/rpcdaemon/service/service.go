package service

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/node"
)

func New(db ethdb.RoKV, ethereum core.EthBackend, engine consensus.Engine, stack *node.Node) {
	var ethashApi *ethash.API
	if casted, ok := engine.(*ethash.Ethash); !ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}
	apis := commands.APIList(context.TODO(), db, core.NewEthBackend(ethereum, ethashApi), nil, cli.Flags{API: []string{"eth", "debug"}}, nil)

	stack.RegisterAPIs(apis)
}
