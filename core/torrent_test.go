package core

import (
	"fmt"
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

	cfg1.ListenHost = torrent.LoopbackListenHost
	cfg1.NoDHT = true
	cfg1.DataDir = dd1
	cfg1.DisableTrackers = true
	cfg1.NoDefaultPortForwarding = true
	cfg1.DisableAcceptRateLimiting = true
	cfg1.ListenPort = 0

	//cfg1.ListenHost=torrent.LoopbackListenHost
	//cfg1.DataDir=dd1
	//cfg1.DisableIPv6=true
	//cfg1.NoUpload=false
	////cfg1.ListenPort = 42069
	//cfg1.Logger = cfg1.Logger.FilterLevel(log.Debug).WithText(func(msg log.Msg) string {
	//	return "seeder " + msg.String()
	//})
	//cfg1.Seed=true
	//cfg1.Debug=true
	//cfg1.NoDHT=true
	//cfg1.DisableTrackers = true
	//cfg1.NoDefaultPortForwarding = true
	//cfg1.DisableAcceptRateLimiting = true

	//cfg1.HeaderObfuscationPolicy=torrent.HeaderObfuscationPolicy{true, true}

	//cfg1.§
	c, err := torrent.NewClient(cfg1)
	if err!=nil {
		t.Fatal(err)
	}
	defer c.Close()
	cfg2:=torrent.NewDefaultClientConfig()
	cfg2.ListenHost = torrent.LoopbackListenHost
	cfg2.NoDHT = true
	cfg2.DataDir = dd2
	cfg2.DisableTrackers = true
	cfg2.NoDefaultPortForwarding = true
	cfg2.DisableAcceptRateLimiting = true
	cfg2.ListenPort = 0

	//cfg2.PublicIp4=net.IP{127,0,0,1}
	//cfg2.ListenHost=func(string) string { return "localhost" }
	//cfg2.ListenHost=torrent.LoopbackListenHost
	//cfg2.DisableIPv6=true
	//cfg2.DataDir=dd2
	////cfg2.HeaderObfuscationPolicy=torrent.HeaderObfuscationPolicy{true, true}
	//
	//cfg2.NoUpload=false
	//cfg2.ListenPort = 42070
	//
	//cfg2.Logger = cfg2.Logger.FilterLevel(log.Debug).WithText(func(msg log.Msg) string {
	//	return "leecher " + msg.String()
	//})
	//
	//cfg2.Seed=true
	//cfg2.Debug=true
	//cfg2.NoDHT=true
	//cfg2.DisableTrackers = true
	//cfg2.NoDefaultPortForwarding = true
	//cfg2.DisableAcceptRateLimiting = true
	//

	c2, err := torrent.NewClient(cfg2)
	if err!=nil {
		t.Fatal(err)
	}
	defer c2.Close()


	mi := metainfo.MetaInfo{
		//AnnounceList: builtinAnnounceList,
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
	fmt.Println("Seeding", trn.Seeding())

	//str:=storage.NewFile("/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir")
	//trn,new:=c.AddTorrentInfoHashWithStorage(metainfo.Hash{},str)

	//fmt.Println("is new", new)
	<-trn.GotInfo()

	time.Sleep(time.Second*10)
	//fmt.Println(trn.)
	fmt.Println("after sleep")
	m2:=mi.Magnet("state trnt", mi.HashInfoBytes())
	//t.Log(m)
	t.Log(m2)

	trn2,err:=c2.AddMagnet(m2.String())
	if err!=nil {
		t.Fatal(err)
	}

	//fmt.Println("connect")
	//trn.AddClientPeer(c2)

	//trn.AddClientPeer(c2)
	trn2.AddClientPeer(c)
	time.Sleep(time.Second*3)
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
	trn2.AllowDataDownload()
	fmt.Println("got info after")
	trn2.DownloadAll()
	fmt.Println("Downloaded all")

	spew.Dump(c2.Torrents()[0].BytesCompleted(), c2.Torrents()[0].Info().TotalLength())
	for i,v :=range c2.Torrents()[0].PieceStateRuns() {
		fmt.Println(i, v.Length, v.Checking, v.Partial, v.Complete, v.Ok, v.Completion, v.PieceState)
	}

	fmt.Println(c2.WaitAll())
	fmt.Println("-Wait all")
	t.Log("trn2",trn2.String())
	time.Sleep(time.Second*10)
	spew.Dump(trn2.Files())
	spew.Dump(trn2.Stats())
	spew.Dump(trn2.Info())
	spew.Dump(trn2.PeerConns())
	spew.Dump(trn2.GotInfo())
	spew.Dump(trn2.Seeding())
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