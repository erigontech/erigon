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
	"runtime"
	"strings"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/c2h5oh/datasize"
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

func New(snapDir string, version string, verbosity lg.Level, downloadRate, uploadRate datasize.ByteSize, port, connsPerFile, downloadSlots int) (*Cfg, error) {
	torrentConfig := Default()
	torrentConfig.ExtendedHandshakeClientVersion = version

	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50
	torrentConfig.DataDir = snapDir

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
	torrentConfig.Logger = lg.Default.FilterLevel(verbosity)
	torrentConfig.Logger.Handlers = []lg.Handler{adapterHandler{}}

	return &Cfg{ClientConfig: torrentConfig, DownloadSlots: downloadSlots}, nil
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
