package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
)

func NotifyRpcDaemon(from, to uint64, notifier *event.Feed, db ethdb.Database) error {
	for i := from; i <= to; i++ {
		hash, err := rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return err
		}
		header := rawdb.ReadHeader(db, hash, i)
		if header == nil {
			return fmt.Errorf("could not find canonical header for hash: %x number: %d", hash, i)
		}
		notifier.Send(ChainHeadEvent{Header: header})
	}
	return nil
}
