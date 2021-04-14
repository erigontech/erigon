package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func NotifyNewHeaders(from, to uint64, notifier ChainEventNotifier, db ethdb.Database) error {
	if notifier == nil {
		log.Warn("rpc notifier is not set, rpc daemon won't be updated about headers")
		return nil
	}
	for i := from; i <= to; i++ {
		header := rawdb.ReadHeaderByNumber(db, i)
		if header == nil {
			return fmt.Errorf("could not find canonical header for number: %d", i)
		}
		notifier.OnNewHeader(header)
	}


	return nil
}
