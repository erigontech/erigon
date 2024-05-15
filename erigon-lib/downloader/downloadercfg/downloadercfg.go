/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package downloadercfg

import (
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/anacrolix/dht/v2"
	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/time/rate"
)

// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
// amount of network announcements, but can't go over 2Mb
// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
const DefaultPieceSize = 2 * 1024 * 1024

// DefaultNetworkChunkSize - how much data request per 1 network call to peer.
// default: 16Kb
const DefaultNetworkChunkSize = 8 * 1024 * 1024

type Cfg struct {
	ClientConfig  *torrent.ClientConfig
	DownloadSlots int

	WebSeedUrls                     []*url.URL
	WebSeedFiles                    []string
	SnapshotConfig                  *snapcfg.Cfg
	DownloadTorrentFilesFromWebseed bool
	AddTorrentsFromDisk             bool
	SnapshotLock                    bool
	ChainName                       string

	Dirs datadir.Dirs
}

func Default() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()
	// better don't increase because erigon periodically producing "new seedable files" - and adding them to downloader.
	// it must not impact chain tip sync - so, limit resources to minimum by default.
	// but when downloader is started as a separated process - rise it to max
	//torrentConfig.PieceHashersPerTorrent = cmp.Max(1, runtime.NumCPU()-1)

	torrentConfig.MinDialTimeout = 6 * time.Second    //default: 3s
	torrentConfig.HandshakesTimeout = 8 * time.Second //default: 4s

	// default limit is 1MB, but we have 2MB pieces which brings us to:
	//   *torrent.PeerConn: waiting for alloc limit reservation: reservation for 1802972 exceeds limiter max 1048576
	torrentConfig.MaxAllocPeerRequestDataPerConn = int64(DefaultPieceSize)

	// this limits the amount of unverified bytes - which will throttle the
	// number of requests the torrent will handle - it acts as a brake on
	// parallelism if set (default is 67,108,864)
	torrentConfig.MaxUnverifiedBytes = 0

	// enable dht
	torrentConfig.NoDHT = true
	//torrentConfig.DisableTrackers = true
	//torrentConfig.DisableWebtorrent = true

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

func New(dirs datadir.Dirs, version string, verbosity lg.Level, downloadRate, uploadRate datasize.ByteSize, port, connsPerFile, downloadSlots int, staticPeers, webseeds []string, chainName string, lockSnapshots bool) (*Cfg, error) {
	torrentConfig := Default()
	//torrentConfig.PieceHashersPerTorrent = runtime.NumCPU()
	torrentConfig.DataDir = dirs.Snap // `DataDir` of torrent-client-lib is different from Erigon's `DataDir`. Just same naming.

	torrentConfig.ExtendedHandshakeClientVersion = version

	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50

	torrentConfig.ListenPort = port
	// check if ipv6 is enabled
	torrentConfig.DisableIPv6 = !getIpv6Enabled()

	if uploadRate > 512*datasize.MB {
		torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Inf, DefaultNetworkChunkSize) // default: unlimited
	} else {
		torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadRate.Bytes()), DefaultNetworkChunkSize) // default: unlimited
	}

	if downloadRate > 512*datasize.MB {
		torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Inf, DefaultNetworkChunkSize) // default: unlimited
	} else {
		torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadRate.Bytes()), DefaultNetworkChunkSize) // default: unlimited
	}

	// debug
	//torrentConfig.Debug = true
	torrentConfig.Logger = torrentConfig.Logger.WithFilterLevel(verbosity)
	torrentConfig.Logger.SetHandlers(adapterHandler{})

	if len(staticPeers) > 0 {
		torrentConfig.NoDHT = false
		//defaultNodes := torrentConfig.DhtStartingNodes
		torrentConfig.DhtStartingNodes = func(network string) dht.StartingNodesGetter {
			return func() ([]dht.Addr, error) {
				addrs, err := dht.GlobalBootstrapAddrs(network)
				if err != nil {
					return nil, err
				}

				for _, seed := range staticPeers {
					if network == "udp" {
						var addr *net.UDPAddr
						addr, err := net.ResolveUDPAddr(network, seed+":80")
						if err != nil {
							log.Warn("[downloader] Cannot UDP resolve address", "network", network, "addr", seed)
							continue
						}
						addrs = append(addrs, dht.NewAddr(addr))
					}
					if network == "tcp" {
						var addr *net.TCPAddr
						addr, err := net.ResolveTCPAddr(network, seed+":80")
						if err != nil {
							log.Warn("[downloader] Cannot TCP resolve address", "network", network, "addr", seed)
							continue
						}
						addrs = append(addrs, dht.NewAddr(addr))
					}
				}
				return addrs, nil
			}
		}
		//staticPeers
	}

	webseedUrlsOrFiles := webseeds
	webseedHttpProviders := make([]*url.URL, 0, len(webseedUrlsOrFiles))
	webseedFileProviders := make([]string, 0, len(webseedUrlsOrFiles))
	for _, webseed := range webseedUrlsOrFiles {
		if !strings.HasPrefix(webseed, "v") { // has marker v1/v2/...
			uri, err := url.ParseRequestURI(webseed)
			if err != nil {
				if strings.HasSuffix(webseed, ".toml") && dir.FileExist(webseed) {
					webseedFileProviders = append(webseedFileProviders, webseed)
				}
				continue
			}
			webseedHttpProviders = append(webseedHttpProviders, uri)
			continue
		}

		if strings.HasPrefix(webseed, "v1:") {
			withoutVerisonPrefix := webseed[3:]
			if !strings.HasPrefix(withoutVerisonPrefix, "https:") {
				continue
			}
			uri, err := url.ParseRequestURI(withoutVerisonPrefix)
			if err != nil {
				log.Warn("[webseed] can't parse url", "err", err, "url", withoutVerisonPrefix)
				continue
			}
			webseedHttpProviders = append(webseedHttpProviders, uri)
		} else {
			continue
		}
	}
	localCfgFile := filepath.Join(dirs.DataDir, "webseed.toml") // datadir/webseed.toml allowed
	if dir.FileExist(localCfgFile) {
		webseedFileProviders = append(webseedFileProviders, localCfgFile)
	}

	return &Cfg{Dirs: dirs, ChainName: chainName,
		ClientConfig: torrentConfig, DownloadSlots: downloadSlots,
		WebSeedUrls: webseedHttpProviders, WebSeedFiles: webseedFileProviders,
		DownloadTorrentFilesFromWebseed: true, AddTorrentsFromDisk: true, SnapshotLock: lockSnapshots,
		SnapshotConfig: snapcfg.KnownCfg(chainName),
	}, nil
}

func getIpv6Enabled() bool {
	if runtime.GOOS == "linux" {
		file, err := os.ReadFile("/sys/module/ipv6/parameters/disable")
		if err != nil {
			log.Warn("could not read /sys/module/ipv6/parameters/disable for ipv6 detection")
			return false
		}
		fileContent := strings.TrimSpace(string(file))
		return fileContent != "0"
	}

	// TODO hotfix: for platforms other than linux disable ipv6
	return false
}
