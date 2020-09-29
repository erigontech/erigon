package generate

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"os"
	"os/signal"
	"time"
)

func SeedSnapshots(dir string) error {
	client := torrent.New(dir, torrent.SnapshotMode{
		Headers:  true,
		Bodies:   true,
		State:    false,
		Receipts: false,
	}, true)

	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	db := ethdb.NewLMDB().Path(dir + "/tmpdb").MustOpen()
	err := client.Run(ethdb.NewObjectDatabase(db))
	if err != nil {
		return err
	}
	torrents := client.Cli.Torrents()
	for _, t := range torrents {
		t := t
		go func() {
			for {
				log.Info("Snapshot stats", "snapshot", t.Name(), "active peers", t.Stats().ActivePeers)
				if common.IsCanceled(ctx) {
					return
				}
				time.Sleep(time.Second * 10)
			}
		}()
	}

	<-ctx.Done()
	return nil
}
