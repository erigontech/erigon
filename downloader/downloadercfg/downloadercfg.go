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
	"io/ioutil"
	"net"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/anacrolix/dht/v2"
	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
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
const DefaultNetworkChunkSize = 512 * 1024

type Cfg struct {
	ClientConfig  *torrent.ClientConfig
	SnapDir       string
	DownloadSlots int
	WebSeedUrls   []*url.URL
	WebSeedFiles  []string
}

func Default() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()

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

func New(dataDir datadir.Dirs, version string, verbosity lg.Level, downloadRate, uploadRate datasize.ByteSize, port, connsPerFile, downloadSlots int, staticPeers []string, webseeds string) (*Cfg, error) {
	torrentConfig := Default()
	torrentConfig.DataDir = dataDir.Snap // `DataDir` of torrent-client-lib is different from Erigon's `DataDir`. Just same naming.

	torrentConfig.ExtendedHandshakeClientVersion = version

	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50

	torrentConfig.ListenPort = port
	// check if ipv6 is enabled
	torrentConfig.DisableIPv6 = !getIpv6Enabled()

	// rates are divided by 2 - I don't know why it works, maybe bug inside torrent lib accounting
	torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadRate.Bytes()), 2*DefaultNetworkChunkSize) // default: unlimited
	if downloadRate.Bytes() < 500_000_000 {
		b := 2 * DefaultNetworkChunkSize
		if downloadRate.Bytes() > DefaultNetworkChunkSize {
			b = int(2 * downloadRate.Bytes())
		}
		torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadRate.Bytes()), b) // default: unlimited
	}

	// debug
	//	torrentConfig.Debug = false
	torrentConfig.Logger.WithFilterLevel(verbosity)
	torrentConfig.Logger.Handlers = []lg.Handler{adapterHandler{}}

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

	webseedUrlsOrFiles := common.CliString2Array(webseeds)
	webseedUrls := make([]*url.URL, 0, len(webseedUrlsOrFiles))
	webseedFiles := make([]string, 0, len(webseedUrlsOrFiles))
	for _, webseed := range webseedUrlsOrFiles {
		uri, err := url.ParseRequestURI(webseed)
		if err != nil {
			if strings.HasSuffix(webseed, ".toml") && dir.FileExist(webseed) {
				webseedFiles = append(webseedFiles, webseed)
			}
			continue
		}
		webseedUrls = append(webseedUrls, uri)
	}
	localCfgFile := filepath.Join(dataDir.DataDir, "webseeds.toml") // datadir/webseeds.toml allowed
	if dir.FileExist(localCfgFile) {
		webseedFiles = append(webseedFiles, localCfgFile)
	}

	return &Cfg{SnapDir: torrentConfig.DataDir,
		ClientConfig: torrentConfig, DownloadSlots: downloadSlots,
		WebSeedUrls: webseedUrls, WebSeedFiles: webseedFiles,
	}, nil
}

func getIpv6Enabled() bool {
	if runtime.GOOS == "linux" {
		file, err := ioutil.ReadFile("/sys/module/ipv6/parameters/disable")
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
