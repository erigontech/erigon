package generate

import (
	"errors"
	"fmt"
	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	trnt "github.com/ledgerwatch/turbo-geth/turbo/torrent"
	"os"
	"os/signal"
	"time"
)

func Seed(pathes []string) error {
	if len(pathes) !=1 {
		return errors.New("you must provide snapshots dir")
	}
	cfg:=torrent.NewDefaultClientConfig()
	cfg.NoDHT=false
	cfg.DisableTrackers=false
	cfg.Seed=true
	cfg.Debug=false
	cfg.Logger=cfg.Logger.FilterLevel(lg.Info)

	cfg.DataDir = pathes[0]
	cfg.DataDir = "/media/b00ris/nvme/snapshots"

	pathes=[]string{
		cfg.DataDir+"/headers_11/",
		cfg.DataDir+"/bodies_11/",
		//cfg.DataDir+"state/",
		//cfg.DataDir+"receipts/",
	}

	//cfg.Logger=cfg.Logger.FilterLevel(trlog.Info)
	cl,err:=torrent.NewClient(cfg)
	if err!=nil {
		return err
	}
	defer cl.Close()

	torrents:=make([]*torrent.Torrent, len(pathes))
	for i,v :=range pathes {
		i:=i
		fmt.Println("i", i)
		mi := &metainfo.MetaInfo{
			CreationDate: time.Now().Unix(),
			CreatedBy:    "turbogeth",
			AnnounceList: trnt.Trackers,
		}

		info := metainfo.Info{PieceLength: trnt.DefaultChunkSize}
		fmt.Println("BuildFromFilePath")
		if _, err := os.Stat(v); os.IsNotExist(err) {
			fmt.Println(err)
			continue
		} else {
			fmt.Println(err)
		}
		err := info.BuildFromFilePath(v)
		if err!=nil {
			return err
		}
		mi.InfoBytes, err = bencode.Marshal(info)
		fmt.Println("AddTorrent")
		fmt.Println("info", common.Bytes2Hex(mi.InfoBytes))

		torrents[i],_,err = cl.AddTorrentSpec(&torrent.TorrentSpec{
			Trackers:trnt.Trackers,
			InfoHash: mi.HashInfoBytes(),
			InfoBytes: mi.InfoBytes,
			//DisplayName: mi.
			ChunkSize: trnt.DefaultChunkSize,
		})
		if err!=nil {
			return err
		}
		if !torrents[i].Seeding() {
			log.Warn(torrents[i].Name()+" not seeding")
		}
		fmt.Println("VerifyData")
		torrents[i].VerifyData()
		go func() {
			tt:=time.Now()
			peerID:=cl.PeerID()
			for {
				fmt.Println(common.Bytes2Hex(peerID[:]),torrents[i].Name(),torrents[i].InfoHash(), torrents[i].PeerConns(),"Swarm", len(torrents[i].KnownSwarm()), torrents[i].Seeding(), time.Since(tt))
				time.Sleep(time.Second*10)
			}
		}()
	}

	c:=make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	<-c
	return nil
}
