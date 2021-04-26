package stagedsync

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/migrator"

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

func MigrateSnapshot(to uint64, tx ethdb.Database, db ethdb.Database, btClient *bittorrent.Client, mg *migrator.SnapshotMigrator2) error {
	headersBlock, err := stages.GetStageProgress(tx, stages.Headers)
	if err!=nil {
		return err
	}

	snBlock:=migrator.CalculateEpoch(headersBlock, 50)

	return mg.Migrate(db, tx, snBlock, btClient)
}