package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func NotifyRpcDaemon(from, to uint64, notifier ChainEventNotifier, db rawdb.DatabaseReader) error {
	if notifier == nil {
		log.Warn("rpc notifier is not set, rpc daemon won't be updated about headers")
		return nil
	}
	for i := from; i <= to; i++ {
		hash, err := rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return err
		}
		header := rawdb.ReadHeader(db, hash, i)
		if header == nil {
			return fmt.Errorf("could not find canonical header for hash: %x number: %d", hash, i)
		}
		notifier.OnNewHeader(header)
	}
	return nil
}
