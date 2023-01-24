package fromdb

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/ethdb/prune"
)

func ChainConfig(db kv.RoDB) (cc *chain.Config) {
	err := db.View(context.Background(), func(tx kv.Tx) error {
		cc = tool.ChainConfig(tx)
		return nil
	})
	tool.Check(err)
	return cc
}

func PruneMode(db kv.RoDB) (pm prune.Mode) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		pm, err = prune.Get(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return
}
