package torrent

import (
	"errors"
	"fmt"
	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"time"
)

const (
	DefaultChunkSizeDefaultChunkSize = 16*1024
	LmdbFilename = "data.mdb"
)

func New(snapshotsDir string, snapshotMode SnapshotMode, seeding bool) *Client  {
	torrentConfig :=torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort=0
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	//torrentConfig.NoDHT = true
	torrentConfig.DisableTrackers = false
	//torrentConfig.Debug=true
	torrentConfig.Logger = torrentConfig.Logger.FilterLevel(lg.Error)
	torrentClient, err := torrent.NewClient(torrentConfig)
	if err!=nil {
		log.Error("Fail to start torrnet client", "err",err)
	}
	return &Client{
		cli: torrentClient,
		snMode: snapshotMode,
		datadir: snapshotsDir,
	}
}

type Client struct {
	cli *torrent.Client
	snMode SnapshotMode
	datadir string
}

func (c *Client) DownloadHeadersSnapshot(db ethdb.Database) error  {
	pc,err:=storage.NewBoltPieceCompletion(c.datadir+"/pieces/"+HeadersSnapshotName)
	if err!=nil {
		return err
	}
	infoBytes,err:=db.Get(dbutils.DatabaseInfoBucket, []byte(HeadersSnapshotName+"_info"))
	if err!=nil && err!=ethdb.ErrKeyNotFound {
		return err
	}
	fmt.Println("Info bytes", common.Bytes2Hex(infoBytes))

	t, new, err:=c.cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:   Trackers,
		InfoHash:    metainfo.NewHashFromHex(HeadersSnapshotHash),
		DisplayName: HeadersSnapshotName,
		ChunkSize:   16*1024,
		Storage: storage.NewFileWithCompletion(c.datadir+"/"+HeadersSnapshotName,pc),
		InfoBytes:   infoBytes,
	})

	if err!=nil {
		return err
	}
	tm:=time.Now()

	gi:
	for {
		select {
		case <-t.GotInfo():
			log.Info("Snapshot information collected", "t", time.Since(tm))
			break gi
		default:
			log.Info("Collecting snapshot info", "t", time.Since(tm))
			time.Sleep(time.Second*10)
		}
	}
	err=db.Put(dbutils.DatabaseInfoBucket, []byte(HeadersSnapshotName+"_info"), t.Metainfo().InfoBytes)
	if err!=nil {
		return err
	}
	t.AllowDataDownload()
	t.DownloadAll()

	tt2:=time.Now()
	dwn:
	for {
		if t.Info().TotalLength()-t.BytesCompleted()==0 {
			log.Info("Dowloaded", "t",time.Since(tt2))
			//fmt.Println("Complete!!!!!!!!!!!!!!!!!!", time.Since(tt2),  t.Info().TotalLength(), t.BytesCompleted())
			break dwn
		} else {
			stats:=t.Stats()
			log.Info("Downloading snapshot", "%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),  "seeders", stats.ConnectedSeeders)
			time.Sleep(time.Second*10)
		}

	}
	return nil
}

func (c *Client) AddTorrent(snapshotName, snapshotHash string) error  {
	pc,err:=storage.NewBoltPieceCompletion(c.datadir+"/pieces/"+snapshotName)
	if err!=nil {
		return err
	}
	t, _, err:=c.cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:   Trackers,
		InfoHash:    metainfo.NewHashFromHex(snapshotHash),
		DisplayName: snapshotName,
		ChunkSize:   DefaultChunkSizeDefaultChunkSize,
		Storage: storage.NewFileWithCompletion(c.datadir+"/"+snapshotName,pc),
	})
	if err!=nil {
		return err
	}
	t.VerifyData()
	t.DisallowDataDownload()
	return nil
}

func (c *Client) WaitGetInfo(hash string) error  {
	t, ok:=c.cli.Torrent(metainfo.NewHashFromHex(hash))
	if !ok {
		return errors.New("not existing torrent")
	}
	t.AllowDataDownload()
	//for i:=range t.Files() {
	//	//t.Files()[i].
	//}
	return nil
}

func (c *Client) DownloadBodiesSnapshot() error  {
	pc,err:=storage.NewBoltPieceCompletion(c.datadir+"/pieces/"+BodiesSnapshotName)
	if err!=nil {
		return err
	}
	t, new, err:=c.cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:   Trackers,
		InfoHash:    metainfo.NewHashFromHex(BlocksSnapshotHash),
		DisplayName: BodiesSnapshotName,
		ChunkSize:   DefaultChunkSizeDefaultChunkSize,
		Storage: storage.NewFileWithCompletion(c.datadir+"/"+BodiesSnapshotName,pc),
	})
	peerID:=c.cli.PeerID()
	fmt.Println(common.Bytes2Hex(peerID[:]),new)
	if err!=nil {
		return err
	}
	tm:=time.Now()

	gi:
	for {
		select {
		case <-t.GotInfo():
			fmt.Println("got info!!!!!!!!!!!!",time.Since(tm))
			break gi
		default:
			fmt.Println("Wait get info", time.Since(tm), t.PeerConns())
			time.Sleep(time.Minute)
		}
	}
	t.AllowDataDownload()
	for i:=range t.Files() {
		t.Files()[i].Download()
	}

	go func() {
		c.cli.WaitAll()
	}()
	tt2:=time.Now()
	dwn:
	for {
		if t.Info().TotalLength()-t.BytesCompleted()==0 {
			fmt.Println("Complete!!!!!!!!!!!!!!!!!!")
			break dwn
		} else {
			fmt.Println(t.BytesMissing(),t.BytesCompleted(), t.Info().TotalLength(), time.Since(tt2), t.PeerConns())
			time.Sleep(time.Second*2)
		}

	}
	return nil
}

//only for mainnet
const (
	HeadersSnapshotName = "headers"
	BodiesSnapshotName = "bodies"
	StateSnapshotName = "state"
	ReceiptsSnapshotName = "receipts"
	//HeadersSnapshotHash = "ab00bf8bc8d159151b35a9c62c6a5c6512187829"
	BlocksSnapshotHash = "0fc6f416651385df347fe05eefae1c26469585a2"
	HeadersSnapshotHash = "7f50f7458715169d98e6dc2f02e2bf52098a3307" //11kk block 1mb block
	//HeadersSnapshotHash = "420e1299b98d391b38a9caa849c4c574ca1d53b3" //11kk block 16k
	//HeadersSnapshotHash = "f291a6986efbc5894840a0fd97e30c5dd38ba4c5"
)


//omplete!!!!!!!!!!!!!!!!!!
//--- PASS: TestTorrentBodies (15997.60s)
var Trackers = [][]string{{
	"udp://tracker.openbittorrent.com:80",
	"udp://tracker.openbittorrent.com:80",
	"udp://tracker.publicbt.com:80",
	"udp://coppersurfer.tk:6969/announce",
	"udp://open.demonii.com:1337",
	"http://bttracker.crunchbanglinux.org:6969/announce",
	"udp://wambo.club:1337/announce",
	"udp://tracker.dutchtracking.com:6969/announce",
	"udp://tc.animereactor.ru:8082/announce",
	"udp://tracker.justseed.it:1337/announce",
	"udp://tracker.leechers-paradise.org:6969/announce",
	"udp://tracker.opentrackr.org:1337/announce",
	"https://open.kickasstracker.com:443/announce",
	"udp://tracker.coppersurfer.tk:6969/announce",
	"udp://open.stealth.si:80/announce",
	"http://87.253.152.137/announce",
	"http://91.216.110.47/announce",
	"http://91.217.91.21:3218/announce",
	"http://91.218.230.81:6969/announce",
	"http://93.92.64.5/announce",
	"http://atrack.pow7.com/announce",
	"http://bt.henbt.com:2710/announce",
	"http://bt.pusacg.org:8080/announce",
	"https://tracker.bt-hash.com:443/announce",
	"udp://tracker.leechers-paradise.org:6969",
	"https://182.176.139.129:6969/announce",
	"udp://zephir.monocul.us:6969/announce",
	"https://tracker.dutchtracking.com:80/announce",
	"https://grifon.info:80/announce",
	"udp://tracker.kicks-ass.net:80/announce",
	"udp://p4p.arenabg.com:1337/announce",
	"udp://tracker.aletorrenty.pl:2710/announce",
	"udp://tracker.sktorrent.net:6969/announce",
	"udp://tracker.internetwarriors.net:1337/announce",
	"https://tracker.parrotsec.org:443/announce",
	"https://tracker.moxing.party:6969/announce",
	"https://tracker.ipv6tracker.ru:80/announce",
	"https://tracker.fastdownload.xyz:443/announce",
	"udp://open.stealth.si:80/announce",
	"https://gwp2-v19.rinet.ru:80/announce",
	"https://tr.kxmp.cf:80/announce",
	"https://explodie.org:6969/announce",
	}, {
	"udp://zephir.monocul.us:6969/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.uw0.xyz:6969/announce",
	"udp://tracker.cyberia.is:6969/announce",
	"http://tracker.files.fm:6969/announce",
	"udp://tracker.zum.bi:6969/announce",
	"http://tracker.nyap2p.com:8080/announce",
	"udp://opentracker.i2p.rocks:6969/announce",
	"udp://tracker.zerobytes.xyz:1337/announce",
	"https://tracker.tamersunion.org:443/announce",
	"https://w.wwwww.wtf:443/announce",
	"https://tracker.imgoingto.icu:443/announce",
	"udp://blokas.io:6969/announce",
	"udp://api.bitumconference.ru:6969/announce",
	"udp://discord.heihachi.pw:6969/announce",
	"udp://cutiegirl.ru:6969/announce",
	"udp://fe.dealclub.de:6969/announce",
	"udp://ln.mtahost.co:6969/announce",
	"udp://vibe.community:6969/announce",
	"http://vpn.flying-datacenter.de:6969/announce",
	"udp://eliastre100.fr:6969/announce",
	"udp://wassermann.online:6969/announce",
	"udp://retracker.local.msn-net.ru:6969/announce",
	"udp://chanchan.uchuu.co.uk:6969/announce",
	"udp://kanal-4.de:6969/announce",
	"udp://handrew.me:6969/announce",
	"udp://mail.realliferpg.de:6969/announce",
	"udp://bubu.mapfactor.com:6969/announce",
	"udp://mts.tvbit.co:6969/announce",
	"udp://6ahddutb1ucc3cp.ru:6969/announce",
	"udp://adminion.n-blade.ru:6969/announce",
	"udp://contra.sf.ca.us:6969/announce",
	"udp://61626c.net:6969/announce",
	"udp://benouworldtrip.fr:6969/announce",
	"udp://sd-161673.dedibox.fr:6969/announce",
	"udp://cdn-1.gamecoast.org:6969/announce",
	"udp://cdn-2.gamecoast.org:6969/announce",
	"udp://daveking.com:6969/announce",
	"udp://bms-hosxp.com:6969/announce",
	"udp://teamspeak.value-wolf.org:6969/announce",
	"udp://edu.uifr.ru:6969/announce",
	"udp://adm.category5.tv:6969/announce",
	"udp://code2chicken.nl:6969/announce",
	"udp://t1.leech.ie:1337/announce",
	"udp://forever-tracker.zooki.xyz:6969/announce",
	"udp://free-tracker.zooki.xyz:6969/announce",
	"udp://public.publictracker.xyz:6969/announce",
	"udp://public-tracker.zooki.xyz:6969/announce",
	"udp://vps2.avc.cx:7171/announce",
	"udp://tracker.fileparadise.in:1337/announce",
	"udp://tracker.skynetcloud.site:6969/announce",
	"udp://z.mercax.com:53/announce",
	"https://publictracker.pp.ua:443/announce",
	"udp://us-tracker.publictracker.xyz:6969/announce",
	"udp://open.stealth.si:80/announce",
	"http://tracker1.itzmx.com:8080/announce",
	"http://vps02.net.orel.ru:80/announce",
	"http://tracker.gbitt.info:80/announce",
	"http://tracker.bt4g.com:2095/announce",
	"https://tracker.nitrix.me:443/announce",
	"udp://aaa.army:8866/announce",
	"udp://tracker.vulnix.sh:6969/announce",
	"udp://engplus.ru:6969/announce",
	"udp://movies.zsw.ca:6969/announce",
	"udp://storage.groupees.com:6969/announce",
	"udp://nagios.tks.sumy.ua:80/announce",
	"udp://tracker.v6speed.org:6969/announce",
	"udp://47.ip-51-68-199.eu:6969/announce",
	"udp://aruacfilmes.com.br:6969/announce",
	"https://trakx.herokuapp.com:443/announce",
	"udp://inferno.demonoid.is:3391/announce",
	"udp://publictracker.xyz:6969/announce",
	"http://tracker2.itzmx.com:6961/announce",
	"http://tracker3.itzmx.com:6961/announce",
	"udp://retracker.akado-ural.ru:80/announce",
	"udp://tracker-udp.gbitt.info:80/announce",
	"http://h4.trakx.nibba.trade:80/announce",
	"udp://tracker.army:6969/announce",
	"http://tracker.anonwebz.xyz:8080/announce",
	"udp://tracker.shkinev.me:6969/announce",
	"http://0205.uptm.ch:6969/announce",
	"udp://tracker.zooki.xyz:6969/announce",
	"udp://forever.publictracker.xyz:6969/announce",
	"udp://tracker.moeking.me:6969/announce",
	"udp://ultra.zt.ua:6969/announce",
	"udp://tracker.publictracker.xyz:6969/announce",
	"udp://ipv4.tracker.harry.lu:80/announce",
	"udp://u.wwwww.wtf:1/announce",
	"udp://line-net.ru:6969/announce",
	"udp://dpiui.reedlan.com:6969/announce",
	"udp://tracker.zemoj.com:6969/announce",
	"udp://t3.leech.ie:1337/announce",
	"http://t.nyaatracker.com:80/announce",
	"udp://exodus.desync.com:6969/announce",
	"udp://valakas.rollo.dnsabr.com:2710/announce",
	"udp://tracker.ds.is:6969/announce",
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://tracker0.ufibox.com:6969/announce",
	"https://tracker.hama3.net:443/announce",
	"udp://opentor.org:2710/announce",
	"udp://t2.leech.ie:1337/announce",
	"https://1337.abcvg.info:443/announce",
	"udp://git.vulnix.sh:6969/announce",
	"udp://retracker.lanta-net.ru:2710/announce",
	"udp://tracker.lelux.fi:6969/announce",
	"udp://bt1.archive.org:6969/announce",
	"udp://admin.videoenpoche.info:6969/announce",
	"udp://drumkitx.com:6969/announce",
	"udp://tracker.dler.org:6969/announce",
	"udp://koli.services:6969/announce",
	"udp://tracker.dyne.org:6969/announce",
	"http://torrenttracker.nwc.acsalaska.net:6969/announce",
	"udp://rutorrent.frontline-mod.com:6969/announce",
	"http://rt.tace.ru:80/announce",
	"udp://explodie.org:6969/announce",
}}