package downloadercfg

import (
	"fmt"
	"net"
	"strings"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/time/rate"
)

// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
// amount of network announcements, but can't go over 2Mb
// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
const DefaultPieceSize = 2 * 1024 * 1024

// DefaultNetworkChunkSize - how much data request per 1 network call to peer.
// default: 16Kb
const DefaultNetworkChunkSize = 1 * 1024 * 1024

type Cfg struct {
	*torrent.ClientConfig
	DownloadSlots int
}

func Default() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()

	// enable dht
	torrentConfig.NoDHT = true
	//torrentConfig.DisableTrackers = true
	//torrentConfig.DisableWebtorrent = true
	//torrentConfig.DisableWebseeds = true

	// Reduce defaults - to avoid peers with very bad geography
	//torrentConfig.MinDialTimeout = 1 * time.Second      // default: 3sec
	//torrentConfig.NominalDialTimeout = 10 * time.Second // default: 20sec
	//torrentConfig.HandshakesTimeout = 1 * time.Second   // default: 4sec

	// see: https://en.wikipedia.org/wiki/TCP_half-open
	//torrentConfig.TotalHalfOpenConns = 100     // default: 100
	//torrentConfig.HalfOpenConnsPerTorrent = 25 // default: 25
	//torrentConfig.TorrentPeersHighWater = 500 // default: 500
	//torrentConfig.TorrentPeersLowWater = 50   // default: 50

	torrentConfig.Seed = true
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"

	return torrentConfig
}

func New(snapDir string, verbosity lg.Level, dbg bool, natif nat.Interface, downloadRate, uploadRate datasize.ByteSize, port, connsPerFile, downloadSlots int) (*Cfg, error) {
	torrentConfig := Default()
	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50
	torrentConfig.DataDir = snapDir

	torrentConfig.ListenPort = port
	// check if ipv6 is enabled
	torrentConfig.DisableIPv6 = true
	l, err := net.Listen("tcp6", fmt.Sprintf(":%d", port))
	if err != nil {
		isDisabled := strings.Contains(err.Error(), "cannot assign requested address") || strings.Contains(err.Error(), "address family not supported by protocol")
		if !isDisabled {
			log.Warn("can't enable ipv6", "err", err)
		}
	} else {
		l.Close()
		torrentConfig.DisableIPv6 = false
	}

	switch natif.(type) {
	case nil:
		// No NAT interface, do nothing.
	case nat.ExtIP:
		// ExtIP doesn't block, set the IP right away.
		ip, _ := natif.ExternalIP()
		if ip != nil {
			if ip.To4() != nil {
				torrentConfig.PublicIp4 = ip
			} else {
				torrentConfig.PublicIp6 = ip
			}
		}
		log.Info("[torrent] Public IP", "ip", ip)

	default:
		// Ask the router about the IP. This takes a while and blocks startup,
		// do it in the background.
		if ip, err := natif.ExternalIP(); err == nil {
			if ip != nil {
				if ip.To4() != nil {
					torrentConfig.PublicIp4 = ip
				} else {
					torrentConfig.PublicIp6 = ip
				}
			}
			log.Info("[torrent] Public IP", "ip", ip)
		}
	}
	// rates are divided by 2 - I don't know why it works, maybe bug inside torrent lib accounting
	torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadRate.Bytes()), 2*DefaultPieceSize) // default: unlimited
	if downloadRate.Bytes() < 500_000_000 {
		b := int(2 * DefaultPieceSize)
		if downloadRate.Bytes() > DefaultPieceSize {
			b = int(2 * downloadRate.Bytes())
		}
		torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadRate.Bytes()), b) // default: unlimited
	}

	// debug
	//	torrentConfig.Debug = false
	torrentConfig.Logger = lg.Default.FilterLevel(verbosity)
	torrentConfig.Logger.Handlers = []lg.Handler{adapterHandler{}}

	return &Cfg{ClientConfig: torrentConfig, DownloadSlots: downloadSlots}, nil
}
