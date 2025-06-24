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
	"errors"
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

	g "github.com/anacrolix/generics"

	analog "github.com/anacrolix/log"

	"github.com/anacrolix/dht/v2"
	"github.com/c2h5oh/datasize"
	"golang.org/x/time/rate"

	"github.com/anacrolix/torrent"
	"github.com/erigontech/erigon-lib/chain/networkname"
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
	ClientConfig *torrent.ClientConfig

	// These are WebSeed URLs conforming to the requirements in anacrolix/torrent.
	WebSeedUrls    []string
	SnapshotConfig *snapcfg.Cfg
	// Deprecated: Call Downloader.AddTorrentsFromDisk or add them yourself. TODO: Remove this.
	// Check with @mh0lt for best way to do this. I couldn't find the GitHub issue for cleaning up
	// the Downloader API and responsibilities.
	AddTorrentsFromDisk bool

	// TODO: Can we get rid of this?
	ChainName string

	Dirs datadir.Dirs

	MdbxWriteMap bool
	// Don't trust any existing piece completion. Revalidate all pieces when added.
	VerifyTorrentData bool
	// Disable automatic data verification in the torrent client. We want to call VerifyData
	// ourselves.
	ManualDataVerification bool
}

// Before options/flags applied.
func defaultTorrentClientConfig() *torrent.ClientConfig {
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
	torrentConfig.UpnpID = torrentConfig.UpnpID + " leecher"

	return torrentConfig
}

// Typed abstraction to make it easier to get config parameters in. Can't pull flags directly due to
// import cycles. cmd/util calls us... Move stuff into this struct as the crazy number of arguments
// annoys you.
type NewCfgOpts struct {
	// If set, clobber the default torrent config value.
	DisableTrackers g.Option[bool]
	Verify          bool
}

func New(
	ctx context.Context,
	dirs datadir.Dirs,
	version string,
	verbosity log.Lvl,
	downloadRate, uploadRate datasize.ByteSize,
	port, connsPerFile int,
	staticPeers, webseeds []string,
	chainName string,
	mdbxWriteMap bool,
	opts NewCfgOpts,
) (_ *Cfg, err error) {
	torrentConfig := defaultTorrentClientConfig()

	if f := opts.DisableTrackers; f.Ok {
		torrentConfig.DisableTrackers = f.Value
	}

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
	slogLevel := erigonToSlogLevel(verbosity)

	// This handler routes messages deemed worth sending to regular Erigon logging machinery, which
	// has filter levels that don't play nice with debugging subsystems with different levels.
	torrentSlogToErigonHandler := slogHandler{
		enabled: func(level slog.Level, names []string) bool {
			return level >= slogLevel
		},
		// We modify the level because otherwise we apply torrent verbosity in the Enabled check,
		// and then downstream Erigon filters with dir or console.
		modifyLevel: func(level *slog.Level, names []string) {
			// Demote tracker messages one whole level.
			//if slices.Contains(names, "tracker") {
			//	*level -= 4
			//}
		},
	}

	torrentSloggerHandlers := multiHandler{&torrentSlogToErigonHandler}

	torrentLogFile, err := os.OpenFile(
		filepath.Join(dirs.DataDir, "logs", "torrent.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err == nil {
		torrentSloggerHandlers = append(
			torrentSloggerHandlers,
			slog.NewJSONHandler(torrentLogFile, &slog.HandlerOptions{
				AddSource:   true,
				Level:       erigonToSlogLevel(verbosity),
				ReplaceAttr: nil,
			}))
	} else {
		log.Error("error opening torrent log file", "err", err)
	}

	torrentConfig.Slogger = slog.New(torrentSloggerHandlers)

	// Check torrent slogger levels and how they route through to erigon log, and anywhere else.
	for _, level := range []slog.Level{
		slog.LevelDebug,
		//slog.LevelInfo,
		//slog.LevelWarn,
		//slog.LevelError,
	} {
		err = torrentConfig.Slogger.Handler().Handle(ctx, slog.NewRecord(time.Now(), level, "test torrent config slogger level", 0))
		if err != nil {
			log.Crit("error testing torrent config slogger level", "err", err, "level", level)
		}
	}
	// Previously this used a logger passed to the callers of this function. Do we need it here?
	log.Info(
		"torrent verbosity",
		"erigon", verbosity,
		// Only for deprecated analog.Logger stuff, if it comes up.
		"anacrolix", analogLevel.LogString(),
		// This should be the one applied to more modern logging in anacrolix/torrent.
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

	// TODO: What do webseed file providers do? Linter warned it was unused, and it appears to be
	// pointless.

	log.Info("processed webseed configuration",
		"webseedHttpProviders", webseedHttpProviders,
		"webseedFileProviders", webseedFileProviders,
		"webseedUrlsOrFiles", webseedUrlsOrFiles)

	// TODO: constructor must not do http requests
	preverifiedCfg, err := LoadSnapshotsHashes(ctx, dirs, chainName)
	if err != nil {
		return nil, err
	}

	cfg := Cfg{
		Dirs:                dirs,
		ChainName:           chainName,
		ClientConfig:        torrentConfig,
		AddTorrentsFromDisk: true,
		SnapshotConfig:      preverifiedCfg,
		MdbxWriteMap:        mdbxWriteMap,
		VerifyTorrentData:   opts.Verify,
	}
	for _, s := range webseedHttpProviders {
		// WebSeed URLs must have a trailing slash if the implementation should append the file
		// name.
		cfg.WebSeedUrls = append(cfg.WebSeedUrls, s.String()+"/")
	}
	return &cfg, nil
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
		snapcfg.SetToml(chainName, haveToml, true)
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
	}
	cfg := snapcfg.KnownCfg(chainName)
	cfg.Local = exists
	return cfg, nil
}

// Saves snapshot hashes. This is done only after the full set of snapshots is completed so that
// clients that don't complete are able to restart from a newer snapshot in case files go missing
// from the network. Should only occur when full preverified snapshot is complete. Probably doesn't
// belong in this package, and neither does LoadSnapshotHashes.
func SaveSnapshotHashes(dirs datadir.Dirs, chainName string) (err error) {
	preverifiedPath := filepath.Join(dirs.Snap, "preverified.toml")
	// TODO: Should the file data be checked to match?
	err = dir.WriteExclusiveFileWithFsync(preverifiedPath, snapcfg.GetToml(chainName), 0o444)
	if errors.Is(err, os.ErrExist) {
		err = nil
	}
	return
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
