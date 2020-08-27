package torrent

import (
	"github.com/anacrolix/torrent"
	"github.com/ledgerwatch/turbo-geth/log"
)

var trackers = []string{
	"udp://tracker.openbittorrent.com:80",
	"udp://tracker.openbittorrent.com:80",
	"udp://tracker.publicbt.com:80",
	"udp://coppersurfer.tk:6969/announce",
	"udp://open.demonii.com:1337",
	"http://bttracker.crunchbanglinux.org:6969/announce",
}

func New(snapshotsDir string, snapshotMode SnapshotMode, seeding bool) *Client  {
	torrentConfig :=torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort=0
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentClient, err := torrent.NewClient(torrentConfig)
	if err!=nil {
		log.Error("Fail to start torrnet client", "err",err)
	}
	return &Client{
		cli: torrentClient,
	}
}

type Client struct {
	cli *torrent.Client
	snMode SnapshotMode
}

func (c *Client) DownloadHeadersSnapshot() error  {
	//c.cli.AddTorrentInfoHash()
	return nil
}

//only for mainnet
const (
	HeadersSnapshotHash = ""
	BlocksSnapshotHash = ""
)