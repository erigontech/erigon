package generate

import (
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/log"

	"os"
	"os/signal"
	"time"
)

func Seed(pathes []string) error {
	cfg:=torrent.NewDefaultClientConfig()
	cfg.DataDir = "/home/b00ris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd/"
	cfg.Seed=true
	//cfg.Logger=cfg.Logger.FilterLevel(trlog.Info)
	cl,err:=torrent.NewClient(cfg)
	if err!=nil {
		return err
	}
	torrents:=make([]*torrent.Torrent, len(pathes))
	for i,v :=range pathes {
		i:=i
		mi := &metainfo.MetaInfo{
			CreationDate: time.Now().Unix(),
			CreatedBy: "turbogeth",
			AnnounceList: builtinAnnounceList,
		}

		info := metainfo.Info{PieceLength: 256 * 1024}
		err := info.BuildFromFilePath(v)
		if err!=nil {
			return err
		}
		mi.InfoBytes, err = bencode.Marshal(info)
		torrents[i],err = cl.AddTorrent(mi)
		if err!=nil {
			return err
		}
		if !torrents[i].Seeding() {
			log.Warn(torrents[i].Name()+" not seeding")
		}
		torrents[i].VerifyData()
		go func() {
			for {
				fmt.Println(torrents[i].Name(),torrents[i].InfoHash(), torrents[i].PeerConns(), torrents[i].Seeding())
				time.Sleep(time.Second*10)
			}
		}()
	}

	c:=make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	<-c
	return nil
}

var (
	builtinAnnounceList = [][]string{
		{"udp://tracker.openbittorrent.com:80"},
		{"udp://tracker.publicbt.com:80"},
		{"udp://tracker.istole.it:6969"},
	}
)

//var trackers = [][]string{
//	{"udp://tracker.openbittorrent.com:80"},
//	"udp://tracker.openbittorrent.com:80",
//	"udp://tracker.publicbt.com:80",
//	"udp://coppersurfer.tk:6969/announce",
//	"udp://open.demonii.com:1337",
//	"http://bttracker.crunchbanglinux.org:6969/announce",
//}