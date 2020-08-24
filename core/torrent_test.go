package core

import (
	"fmt"
	"github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
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

func TestNameSeed(t *testing.T) {

}

func TestName(t *testing.T) {
	t.Skip()
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
	cfg1.Seed=true
	//cfg1.Debug=true
	cfg1.NoDHT=true
	cfg1.DisableTrackers = true
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
	cfg2.DisableTrackers=true
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
	cfg2.NoDHT=true
	//cfg2.DisableTrackers = true
	//cfg2.NoDefaultPortForwarding = true
	//cfg2.DisableAcceptRateLimiting = true
	//

	c2, err := torrent.NewClient(cfg2)
	if err!=nil {
		t.Fatal(err)
	}
	defer c2.Close()

	time.Sleep(time.Second)
	fmt.Println("make magnet+")
	magnet:=makeMagnet(t, c, "/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir", "eln.zip")
	fmt.Println("make magnet-")
	//mi := metainfo.MetaInfo{
	//	//AnnounceList: builtinAnnounceList,
	//}
	//mi.SetDefaults()
	//mi.Comment = "lib test"
	//mi.CreatedBy = "tg"
	//
	//info := metainfo.Info{
	//	PieceLength: 256 * 1024,
	//}
	//err = info.BuildFromFilePath("/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir/")
	//if err != nil {
	//	t.Fatal(err)
	//}
	//spew.Dump(info)
	//
	//mi.InfoBytes, err = bencode.Marshal(info)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//trn,err:=c.AddTorrent(&mi)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//trn.VerifyData()
	//fmt.Println("Seeding", trn.Seeding())

	//str:=storage.NewFile("/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir")
	//trn,new:=c.AddTorrentInfoHashWithStorage(metainfo.Hash{},str)

	//fmt.Println("is new", new)
	//<-trn.GotInfo()

	//time.Sleep(time.Second*10)
	////fmt.Println(trn.)
	//fmt.Println("after sleep")
	//m2:=mi.Magnet("state trnt", mi.HashInfoBytes())
	////t.Log(m)
	//t.Log(m2)

	trn2,err:=c2.AddMagnet(magnet)
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
			time.Sleep(time.Second*5)
		}

	}
	trn2.VerifyData()
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
	t.Skip()
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

func TestName4(t *testing.T) {
	t.Skip()

	dd1:="/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd1"
	dd2:="/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd2"
	os.RemoveAll(dd1)
	os.RemoveAll(dd2)

	cfg := TestingConfig(dd1)
	cfg.Seed = true
	cfg.DataDir = filepath.Join(cfg.DataDir, "server")
	os.Mkdir(cfg.DataDir, 0755)
	//seeder(cfg)
	server, err := torrent.NewClient(cfg)
	require.NoError(t, err)
	//defer server.Close()
	//defer testutil.ExportStatusWriter(server, "s")()
	magnet1 := makeMagnet(t, server, cfg.DataDir, "eln.zip")
	cfg = TestingConfig(dd2)
	cfg.DataDir = filepath.Join(cfg.DataDir, "client")
	//leecher(cfg)
	client, err := torrent.NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()

	tr, err := client.AddMagnet(magnet1)
	require.NoError(t, err)
	tr.AddClientPeer(server)
	<-tr.GotInfo()
	tr.DownloadAll()
	client.WaitAll()
}

func TestName5(t *testing.T) {
	dd1:="/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd1"
	//dd2:="/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/dd2"
	//os.RemoveAll(dd1)
	//os.RemoveAll(dd2)

	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir=dd1
	cfg.Logger=cfg.Logger.FilterLevel(log.Info)
	cfg.NoDHT = false
	cfg.DisableTrackers = false
	cfg.Seed = true
	cfg.DataDir = filepath.Join(cfg.DataDir, "server")
	//os.Mkdir(cfg.DataDir, 0755)
	//seeder(cfg)
	server, err := torrent.NewClient(cfg)
	require.NoError(t, err)
	//defer server.Close()
	//defer testutil.ExportStatusWriter(server, "s")()
	magnet1 := makeMagnet(t, server, cfg.DataDir, "ulc_long.mp4")
	fmt.Println(magnet1)
	c:=make(chan struct{})
	<-c
	//cfg = TestingConfig(dd2)
	//cfg.DataDir = filepath.Join(cfg.DataDir, "client")
	////leecher(cfg)
	//client, err := torrent.NewClient(cfg)
	//require.NoError(t, err)
	//defer client.Close()
	//
	//tr, err := client.AddMagnet(magnet1)
	//require.NoError(t, err)
	//tr.AddClientPeer(server)
	//<-tr.GotInfo()
	//tr.DownloadAll()
	//client.WaitAll()
}
func makeMagnet(t *testing.T, cl *torrent.Client, dir string, name string) string {
	//to:=dir+"/eln.zip"
	//fmt.Println("copy to", to)
	//_,err:=fileutils.CopyFile("/Users/boris/go/src/github.com/ledgerwatch/turbo-geth/debug/trndir/eln.zip", to)
	//require.NoError(t, err)

	mi := metainfo.MetaInfo{}
	mi.SetDefaults()
	mi.AnnounceList=builtinAnnounceList
	info := metainfo.Info{PieceLength: 256 * 1024}
	err := info.BuildFromFilePath(filepath.Join(dir, name))
	require.NoError(t, err)
	mi.InfoBytes, err = bencode.Marshal(info)
	require.NoError(t, err)
	magnet := mi.Magnet(name, mi.HashInfoBytes()).String()
	tr, err := cl.AddTorrent(&mi)
	require.NoError(t, err)
	require.True(t, tr.Seeding())
	tr.VerifyData()
	go func() {
		for {
			fmt.Println(tr.PeerConns(), tr.Seeding())
			time.Sleep(time.Second*60)

		}
	}()
	return magnet
}

//func makeMagnet(t *testing.T, cl *torrent.Client, dir string, name string) string {
//	mi := metainfo.MetaInfo{}
//	mi.SetDefaults()
//	info := metainfo.Info{PieceLength: 256*1024}
//	fmt.Println("build from path")
//	err := info.BuildFromFilePath(dir)
//	require.NoError(t, err)
//	mi.InfoBytes, err = bencode.Marshal(info)
//	require.NoError(t, err)
//	fmt.Println("magnet")
//	fmt.Println("add torrnet")
//	tr, err := cl.AddTorrent(&mi)
//	require.NoError(t, err)
//	require.True(t, tr.Seeding())
//	fmt.Println("verify")
//	<-tr.GotInfo()
//	tr.VerifyData()
//	magnet := mi.Magnet(name, mi.HashInfoBytes()).String()
//	return magnet
//}


func TestingConfig(dir string) *torrent.ClientConfig {
	cfg := torrent.NewDefaultClientConfig()
	cfg.ListenHost = torrent.LoopbackListenHost
	cfg.NoDHT = true
	cfg.DataDir = dir
	cfg.DisableTrackers = true
	cfg.NoDefaultPortForwarding = true
	cfg.DisableAcceptRateLimiting = true
	cfg.ListenPort = 0
	//cfg.Debug = true
	//cfg.Logger = cfg.Logger.WithText(func(m log.Msg) string {
	//	t := m.Text()
	//	m.Values(func(i interface{}) bool {
	//		t += fmt.Sprintf("\n%[1]T: %[1]v", i)
	//		return true
	//	})
	//	return t
	//})
	return cfg
}
