package core

import (
	"fmt"
	"github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/davecgh/go-spew/spew"
	"os"
	"testing"
	"time"
)

var (
	builtinAnnounceList = [][]string{
		{"udp://tracker.openbittorrent.com:80"},
		{"udp://tracker.publicbt.com:80"},
		{"udp://tracker.istole.it:6969"},
	}
)

func TestName(t *testing.T) {
	dd1:="/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd1"
	dd2:="/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd2"
	os.RemoveAll(dd1)
	os.RemoveAll(dd2)
	cfg1:=torrent.NewDefaultClientConfig()
	cfg1.DataDir=dd1
	cfg1.ListenPort = 42069
	cfg1.Logger = cfg1.Logger.FilterLevel(log.Info).WithValues("seeder")
	cfg1.Seed=true
	//cfg1.§
	c, err := torrent.NewClient(cfg1)
	if err!=nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg2:=torrent.NewDefaultClientConfig()
	cfg2.DataDir=dd2
	cfg2.ListenPort = 42070
	cfg2.Logger = cfg2.Logger.FilterLevel(log.Info)

	c2, err := torrent.NewClient(cfg2)
	if err!=nil {
		t.Fatal(err)
	}
	defer c2.Close()


	mi := metainfo.MetaInfo{
		AnnounceList: builtinAnnounceList,
	}
	mi.SetDefaults()
	mi.Comment = "lib test"
	mi.CreatedBy = "tg"

	info := metainfo.Info{
		PieceLength: 256 * 1024,
	}
	err = info.BuildFromFilePath("/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir/")
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(info)
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	trn,err:=c.AddTorrent(&mi)
	if err != nil {
		t.Fatal(err)
	}

	//str:=storage.NewFile("/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir")
	//trn,new:=c.AddTorrentInfoHashWithStorage(metainfo.Hash{},str)

	//fmt.Println("is new", new)
	<-trn.GotInfo()


	m2:=mi.Magnet("state trnt", mi.HashInfoBytes())
	//t.Log(m)
	t.Log(m2)
	trn2,err:=c2.AddMagnet(m2.String())
	if err!=nil {
		t.Fatal(err)
	}
	trn2.AddClientPeer(c)
	fmt.Println("got info")
	gotinfo:
	for {
		select {
		case <-trn2.GotInfo():
			spew.Dump(trn2.Files())
			spew.Dump(trn2.Stats())

			break gotinfo
		default:
			fmt.Println(trn2)
			fmt.Println(trn2.PeerConns())
			time.Sleep(time.Second*10)
		}

	}
	fmt.Println("got info after")
	trn2.DownloadAll()
	fmt.Println("Downloaded all")
	spew.Dump(trn2.PieceStateRuns())
	spew.Dump(c2.Torrents())
	c2.WaitAll()
	fmt.Println("Wait all")
	t.Log("trn2",trn2.String())
	time.Sleep(time.Minute)
	//trn.InfoHash().
	//t, _ := c.AddMagnet("magnet:?xt=urn:btih:ZOCMZQIPFFW7OLLMIC5HUB6BPCSDEOQU")
	//<-trn.GotInfo()
	//trn.DownloadAll()
	//c.WaitAll()
	//log.Print("ermahgerd, torrent downloaded")
	//mi := metainfo.MetaInfo{
	//	AnnounceList: builtinAnnounceList,
	//}
	//
	//mi.AnnounceList = make([][]string, 0)
	//mi.AnnounceList = append(mi.AnnounceList, []string{a})
	//mi.SetDefaults()
	//mi.Comment = args.Comment
	//mi.CreatedBy = args.CreatedBy
	//
	//info := metainfo.Info{
	//	PieceLength: 256 * 1024,
	//}
	//err := info.BuildFromFilePath(args.Root)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//mi.InfoBytes, err = bencode.Marshal(info)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//err = mi.Write(os.Stdout)
	//if err != nil {
	//	log.Fatal(err)
	//}
}


func TestName2(t *testing.T) {
	cfg:=torrent.NewDefaultClientConfig()
	cfg.DataDir = "/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd3"
	cfg.Debug=false
	c, err := torrent.NewClient(cfg)
	if err!=nil {
		t.Fatal(err)
	}
	defer c.Close()
	t1, err := c.AddMagnet("magnet:?xt=urn:btih:ZOCMZQIPFFW7OLLMIC5HUB6BPCSDEOQU")
	if err!=nil {
		t.Fatal(err)
	}
	<-t1.GotInfo()
	t1.DownloadAll()
	c.WaitAll()
	t.Log("ermahgerd, torrent downloaded")
}