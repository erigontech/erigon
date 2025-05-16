// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package downloadercfg

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	analog "github.com/anacrolix/log"

	"github.com/erigontech/erigon-lib/chain/networkname"

	"github.com/anacrolix/dht/v2"
	"github.com/c2h5oh/datasize"
	"golang.org/x/time/rate"

	"github.com/anacrolix/torrent"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
)

// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
// amount of network announcements, but can't go over 2Mb. TODO: This is definitely not true.
// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
const DefaultPieceSize = 2 * 1024 * 1024

// DefaultNetworkChunkSize - how much data request per 1 network call to peer.
// default: 16Kb
const DefaultNetworkChunkSize = 256 << 10

type Cfg struct {
	ClientConfig  *torrent.ClientConfig
	DownloadSlots int

	WebSeedUrls                     []*url.URL
	WebSeedFileProviders            []string
	SnapshotConfig                  *snapcfg.Cfg
	DownloadTorrentFilesFromWebseed bool
	// TODO: Have I rendered this obsolete?
	AddTorrentsFromDisk bool

	ChainName string

	Dirs datadir.Dirs

	MdbxWriteMap bool
}

func Default() *torrent.ClientConfig {
	// TODO: Add config dump in client status writer output...
	torrentConfig := torrent.NewDefaultClientConfig()
	// better don't increase because erigon periodically producing "new seedable files" - and adding them to downloader.
	// it must not impact chain tip sync - so, limit resources to minimum by default.
	// but when downloader is started as a separated process - rise it to max
	torrentConfig.PieceHashersPerTorrent = dbg.EnvInt("DL_HASHERS", min(16, max(2, runtime.NumCPU()-2)))

	torrentConfig.MinDialTimeout = 6 * time.Second    //default: 3s
	torrentConfig.HandshakesTimeout = 8 * time.Second //default: 4s

	// This needs to be at least the chunk size of requests we expect to service for peers. This has
	// been as high as 8 MiB unintentionally, but the piece size for all previous torrents has been
	// 2 MiB. Therefore, 2 MiB is required to service those nodes. TODO: Reevaluate this in Erigon
	// 3.2 when there are no 3.0 users.
	torrentConfig.MaxAllocPeerRequestDataPerConn = max(torrentConfig.MaxAllocPeerRequestDataPerConn, DefaultPieceSize)

	// enable dht. TODO: We want DHT.
	torrentConfig.NoDHT = true

	torrentConfig.Seed = true
	// TODO: Check the result of this in the client status spew.
	torrentConfig.UpnpID = torrentConfig.UpnpID + " leecher"

	return torrentConfig
}

func New(
	ctx context.Context,
	dirs datadir.Dirs,
	version string,
	verbosity log.Lvl,
	downloadRate, uploadRate datasize.ByteSize,
	port, connsPerFile, downloadSlots int,
	staticPeers, webseeds []string,
	chainName string,
	mdbxWriteMap bool,
) (_ *Cfg, err error) {
	torrentConfig := Default()
	//torrentConfig.PieceHashersPerTorrent = runtime.NumCPU()
	torrentConfig.DataDir = dirs.Snap // `DataDir` of torrent-client-lib is different from Erigon's `DataDir`. Just same naming.

	torrentConfig.ExtendedHandshakeClientVersion = version

	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50

	torrentConfig.ListenPort = port
	// check if ipv6 is enabled
	torrentConfig.DisableIPv6 = !getIpv6Enabled()

	torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadRate.Bytes()), 0)
	torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadRate.Bytes()), 0)

	var analogLevel analog.Level
	analogLevel, torrentConfig.Debug, err = erigonToAnalogLevel(verbosity)
	if err != nil {
		panic(err)
	}
	torrentConfig.Logger = torrentConfig.Logger.WithFilterLevel(analogLevel)
	torrentConfig.Logger.SetHandlers(adapterHandler{})
	slogLevel := erigonToSlogLevel(verbosity)
	torrentConfig.Slogger = slog.New(&slogHandler{
		enabled: func(level slog.Level, names []string) bool {
			if slices.Contains(names, "tracker") {
				level -= 4
			}
			return level >= slogLevel
		},
	})
	torrentConfig.Logger.Levelf(analog.Debug, "test")
	torrentConfig.Slogger.Debug("test")
	// Previously this used a logger passed to the callers of this function. Do we need it here?
	log.Info(
		"torrent verbosity",
		"erigon", verbosity,
		"anacrolix", analogLevel.LogString(),
		"slog", slogLevel)

	// TODO: This doesn't look right. Enabling DHT only for static peers will introduce very
	// different runtime behaviour. Is it assumed those peers are torrent client endpoints and we
	// want to force connecting to them? PEX might be more suited here. DHT should eventually be
	// enabled regardless, but then this code would make more sense.
	if len(staticPeers) > 0 && false {
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
						addr, err := net.ResolveUDPAddr(network, seed)
						if err != nil {
							log.Warn("[downloader] Cannot UDP resolve address", "network", network, "addr", seed)
							continue
						}
						addrs = append(addrs, dht.NewAddr(addr))
					}
					if network == "tcp" {
						var addr *net.TCPAddr
						addr, err := net.ResolveTCPAddr(network, seed)
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
				exists, err := dir.FileExist(webseed)
				if err != nil {
					log.Warn("[webseed] FileExist error", "err", err)
					continue
				}
				if strings.HasSuffix(webseed, ".toml") && exists {
					webseedFileProviders = append(webseedFileProviders, webseed)
				}
				continue
			}
			webseedHttpProviders = append(webseedHttpProviders, uri)
			continue
		}

		if strings.HasPrefix(webseed, "v1:") {
			withoutVersionPrefix := webseed[3:]
			if !strings.HasPrefix(withoutVersionPrefix, "https:") {
				continue
			}
			uri, err := url.ParseRequestURI(withoutVersionPrefix)
			if err != nil {
				log.Warn("[webseed] can't parse url", "err", err, "url", withoutVersionPrefix)
				continue
			}
			webseedHttpProviders = append(webseedHttpProviders, uri)
		} else {
			continue
		}
	}
	localCfgFile := filepath.Join(dirs.DataDir, "webseed.toml") // datadir/webseed.toml allowed
	exists, err := dir.FileExist(localCfgFile)
	if err != nil {
		log.Error("[webseed] FileExist error", "err", err)
		return nil, err
	}
	if exists {
		webseedFileProviders = append(webseedFileProviders, localCfgFile)
	}

	// TODO: constructor must not do http requests
	preverifiedCfg, err := LoadSnapshotsHashes(ctx, dirs, chainName)
	if err != nil {
		return nil, err
	}

	return &Cfg{
		Dirs:                            dirs,
		ChainName:                       chainName,
		ClientConfig:                    torrentConfig,
		DownloadSlots:                   downloadSlots,
		WebSeedUrls:                     webseedHttpProviders,
		WebSeedFileProviders:            webseedFileProviders,
		DownloadTorrentFilesFromWebseed: true,
		AddTorrentsFromDisk:             true,
		SnapshotConfig:                  preverifiedCfg,
		MdbxWriteMap:                    mdbxWriteMap,
	}, nil
}

// LoadSnapshotsHashes checks local preverified.toml. If file exists, used local hashes.
// If there are no such file, try to fetch hashes from the web and create local file.
func LoadSnapshotsHashes(ctx context.Context, dirs datadir.Dirs, chainName string) (*snapcfg.Cfg, error) {
	if !slices.Contains(networkname.All, chainName) {
		log.Root().Warn("No snapshot hashes for chain", "chain", chainName)
		return snapcfg.NewNonSeededCfg(chainName), nil
	}

	preverifiedPath := filepath.Join(dirs.Snap, "preverified.toml")
	exists, err := dir.FileExist(preverifiedPath)
	if err != nil {
		return nil, err
	}
	if exists {
		// Load hashes from local preverified.toml
		haveToml, err := os.ReadFile(preverifiedPath)
		if err != nil {
			return nil, err
		}
		snapcfg.SetToml(chainName, haveToml)
	} else {
		// Fetch the snapshot hashes from the web
		fetched, err := snapcfg.LoadRemotePreverified(ctx)
		if err != nil {
			log.Root().Crit("Snapshot hashes for supported networks was not loaded. Please check your network connection and/or GitHub status here https://www.githubstatus.com/", "chain", chainName, "err", err)
			return nil, fmt.Errorf("failed to fetch remote snapshot hashes for chain %s", chainName)
		}
		if !fetched {
			log.Root().Crit("Snapshot hashes for supported networks was not loaded. Please check your network connection and/or GitHub status here https://www.githubstatus.com/", "chain", chainName)
			return nil, fmt.Errorf("remote snapshot hashes was not fetched for chain %s", chainName)
		}
		if err := dir.WriteFileWithFsync(preverifiedPath, snapcfg.GetToml(chainName), 0644); err != nil {
			return nil, err
		}
	}
	return snapcfg.KnownCfg(chainName), nil
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
