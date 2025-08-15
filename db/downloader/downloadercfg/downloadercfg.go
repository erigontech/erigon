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
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/time/rate"

	g "github.com/anacrolix/generics"
	analog "github.com/anacrolix/log"
	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/anacrolix/torrent"
	pp "github.com/anacrolix/torrent/peer_protocol"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapcfg"
)

// DefaultPieceSize - Erigon serves many big files, bigger pieces will reduce
// amount of network announcements, but can't go over 2Mb. TODO: This is definitely not true.
// see https://wiki.theory.org/BitTorrentSpecification#Metainfo_File_Structure
const DefaultPieceSize = 2 * 1024 * 1024

// DefaultNetworkChunkSize - how much data request per 1 network call to peer.
// BitTorrent client default: 16Kb
var NetworkChunkSize pp.Integer = 256 << 10 // 256 KiB

func init() {
	s := os.Getenv("DOWNLOADER_NETWORK_CHUNK_SIZE")
	if s == "" {
		return
	}
	i64, err := strconv.ParseInt(s, 10, 0)
	panicif.Err(err)
	NetworkChunkSize = pp.Integer(i64)
}

type Cfg struct {
	Dirs datadir.Dirs
	// Separate rate limit for webseeds.
	SeparateWebseedDownloadRateLimit g.Option[rate.Limit]
	// These are WebSeed URLs conforming to the requirements in anacrolix/torrent.
	WebSeedUrls []string

	// TODO: Can we get rid of this?
	ChainName string

	ClientConfig   *torrent.ClientConfig
	SnapshotConfig *snapcfg.Cfg

	// Deprecated: Call Downloader.AddTorrentsFromDisk or add them yourself. TODO: RemoveFile this.
	// Check with @mh0lt for best way to do this. I couldn't find the GitHub issue for cleaning up
	// the Downloader API and responsibilities.
	AddTorrentsFromDisk bool

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
	//torrentConfig.PieceHashersPerTorrent = dbg.EnvInt("DL_HASHERS", min(16, max(2, runtime.NumCPU()-2)))

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
	DisableTrackers          g.Option[bool]
	Verify                   bool
	UploadRateLimit          g.Option[rate.Limit]
	DownloadRateLimit        g.Option[rate.Limit]
	WebseedDownloadRateLimit g.Option[rate.Limit]
}

func New(
	ctx context.Context,
	dirs datadir.Dirs,
	version string,
	verbosity log.Lvl,
	port, connsPerFile int,
	webseeds []string,
	chainName string,
	mdbxWriteMap bool,
	opts NewCfgOpts,
) (_ *Cfg, err error) {
	torrentConfig := defaultTorrentClientConfig()

	torrentConfig.MaxUnverifiedBytes = 0

	torrentConfig.MetainfoSourcesMerger = func(t *torrent.Torrent, info *metainfo.MetaInfo) error {
		return t.SetInfoBytes(info.InfoBytes)
	}

	//torrentConfig.PieceHashersPerTorrent = runtime.NumCPU()
	torrentConfig.DataDir = dirs.Snap // `DataDir` of torrent-client-lib is different from Erigon's `DataDir`. Just same naming.

	torrentConfig.ExtendedHandshakeClientVersion = version

	// We would-like to reduce amount of goroutines in Erigon, so reducing next params
	torrentConfig.EstablishedConnsPerTorrent = connsPerFile // default: 50

	torrentConfig.ListenPort = port
	// check if ipv6 is enabled
	torrentConfig.DisableIPv6 = !getIpv6Enabled()

	if opts.UploadRateLimit.Ok {
		torrentConfig.UploadRateLimiter = rate.NewLimiter(opts.UploadRateLimit.Value, 0)
	}
	for value := range opts.DownloadRateLimit.Iter() {
		switch value {
		case rate.Inf:
			torrentConfig.DownloadRateLimiter = nil
		case 0:
			torrentConfig.DialForPeerConns = false
			torrentConfig.AcceptPeerConnections = false
			torrentConfig.DisableTrackers = true
			fallthrough
		default:
			torrentConfig.DownloadRateLimiter = rate.NewLimiter(value, 0)
		}
	}

	// Override value set by download rate-limit.
	for value := range opts.DisableTrackers.Iter() {
		torrentConfig.DisableTrackers = value
	}

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
				Level:       min(erigonToSlogLevel(verbosity), slog.LevelWarn),
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

	webseedUrlsOrFiles := webseeds
	webseedHttpProviders := make([]*url.URL, 0, len(webseedUrlsOrFiles))
	for _, webseed := range webseedUrlsOrFiles {
		if !strings.HasPrefix(webseed, "v") { // has marker v1/v2/...
			uri, err := url.ParseRequestURI(webseed)
			if err != nil {
				log.Warn("[webseed]", "can't parse url", "err", err, "url", webseed)
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

	log.Info("processed webseed configuration",
		"webseedHttpProviders", webseedHttpProviders,
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

	for value := range opts.WebseedDownloadRateLimit.Iter() {
		cfg.SeparateWebseedDownloadRateLimit.Set(value)
	}

	return &cfg, nil
}

// LoadSnapshotsHashes checks local preverified.toml. If file exists, used local hashes.
// If there are no such file, try to fetch hashes from the web and create local file.
func LoadSnapshotsHashes(ctx context.Context, dirs datadir.Dirs, chainName string) (*snapcfg.Cfg, error) {
	if _, known := snapcfg.KnownCfg(chainName); !known {
		log.Root().Warn("No snapshot hashes for chain", "chain", chainName)
		return snapcfg.NewNonSeededCfg(chainName), nil
	}

	preverifiedPath := dirs.PreverifiedPath()
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
		err := snapcfg.LoadRemotePreverified(ctx)
		if err != nil {
			log.Root().Crit("Snapshot hashes for supported networks was not loaded. Please check your network connection and/or GitHub status here https://www.githubstatus.com/", "chain", chainName, "err", err)
			return nil, fmt.Errorf("failed to fetch remote snapshot hashes for chain %s", chainName)
		}
	}
	cfg, _ := snapcfg.KnownCfg(chainName)
	cfg.Local = exists
	return cfg, nil
}

// Saves snapshot hashes. This is done only after the full set of snapshots is completed so that
// clients that don't complete are able to restart from a newer snapshot in case files go missing
// from the network. Should only occur when full preverified snapshot is complete. Probably doesn't
// belong in this package, and neither does LoadSnapshotHashes.
func SaveSnapshotHashes(dirs datadir.Dirs, chainName string) (err error) {
	// TODO: Should the file data be checked to match?
	err = dir.WriteExclusiveFileWithFsync(dirs.PreverifiedPath(), snapcfg.GetToml(chainName), 0o444)
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
