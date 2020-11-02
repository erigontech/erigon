package commands

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"os"
	"os/signal"
	"time"
)

func SeedSnapshots(dir string) error {
	client := bittorrent.New(dir, true)

	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	db := ethdb.NewLMDB().Path(dir + "/tmpdb").MustOpen()
	//todo
	err := client.AddSnapshotsTorrents(ethdb.NewObjectDatabase(db), 1, snapshotsync.SnapshotMode{})
	//err := client.Run(ethdb.NewObjectDatabase(db))
	if err != nil {
		return err
	}
	client.Download()

	//Seeding
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			for _, t := range client.Cli.Torrents() {
				log.Info("Snapshot stats", "snapshot", t.Name(), "active peers", t.Stats().ActivePeers, "seeding", t.Seeding())
			}
			if common.IsCanceled(ctx) {
				ticker.Stop()
				return
			}
		}
	}()

	<-ctx.Done()
	return nil
}
