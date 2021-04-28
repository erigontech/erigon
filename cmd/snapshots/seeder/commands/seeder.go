package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	trnt "github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
)

func Seed(ctx context.Context, datadir string) error {
	defer func() {
		//hack origin lib don't have proper close handling
		time.Sleep(time.Second*5)
	}()
	datadir = filepath.Dir(datadir)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := trnt.DefaultTorrentConfig()
	cfg.NoDHT = false
	cfg.DisableTrackers = false
	cfg.Seed = true
	cfg.Debug = false
	cfg.Logger = cfg.Logger.FilterLevel(lg.Info)
	cfg.DataDir = datadir

	pathes := []string{
		cfg.DataDir + "/headers",
		cfg.DataDir + "/bodies",
		cfg.DataDir + "/state",
	}

	cl, err := torrent.NewClient(cfg)
	if err != nil {
		return err
	}
	defer cl.Close()

	torrents := make([]*torrent.Torrent, len(pathes))
	for i, v := range pathes {
		i := i
		mi := &metainfo.MetaInfo{
			CreationDate: time.Now().Unix(),
			CreatedBy:    "turbogeth",
			AnnounceList: trnt.Trackers,
		}

		if _, err := os.Stat(v); os.IsNotExist(err) {
			fmt.Println(err)
			continue
		} else if err != nil {
			return err
		}
		tt := time.Now()
		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}
		info, err := trnt.BuildInfoBytesForSnapshot(v, trnt.LmdbFilename)
		if err != nil {
			return err
		}

		mi.InfoBytes, err = bencode.Marshal(info)
		if err != nil {
			return err
		}

		torrents[i], _, err = cl.AddTorrentSpec(&torrent.TorrentSpec{
			Trackers:  trnt.Trackers,
			InfoHash:  mi.HashInfoBytes(),
			InfoBytes: mi.InfoBytes,
			ChunkSize: trnt.DefaultChunkSize,
		})
		if err != nil {
			return err
		}

		log.Info("Torrent added", "name", torrents[i].Info().Name, "path", v, "t", time.Since(tt))

		if !torrents[i].Seeding() {
			log.Warn(torrents[i].Name() + " not seeding")
		}

		if common.IsCanceled(ctx) {
			return common.ErrStopped
		}
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			for _, t := range cl.Torrents() {
				log.Info("Snapshot stats", "snapshot", t.Name(), "active peers", t.Stats().ActivePeers, "seeding", t.Seeding(), "hash", t.Metainfo().HashInfoBytes().String())
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
