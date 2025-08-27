// Copyright 2024 The Erigon Authors
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

package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/go-viper/mapstructure/v2"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/downloader/downloadernat"
	"github.com/erigontech/erigon/cmd/hack/tool"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/downloader/downloadergrpc"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/version"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/paths"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains

	_ "github.com/erigontech/erigon/db/snaptype2"     //hack
	_ "github.com/erigontech/erigon/polygon/heimdall" //hack
)

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var (
	webseeds                       string
	datadirCli, chain              string
	filePath                       string
	forceRebuild                   bool
	verify                         bool
	verifyFailfast                 bool
	_verifyFiles                   string
	verifyFiles                    []string
	downloaderApiAddr              string
	natSetting                     string
	torrentVerbosity               int
	downloadRateStr, uploadRateStr string
	// How do I mark this deprecated with cobra?
	torrentDownloadSlots int
	staticPeersStr       string
	torrentPort          int
	torrentMaxPeers      int
	torrentConnsPerFile  int
	targetFile           string
	disableIPV6          bool
	disableIPV4          bool
	seedbox              bool
	dbWritemap           bool
	all                  bool
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)

	withDataDir(rootCmd)
	withChainFlag(rootCmd)

	rootCmd.Flags().StringVar(&webseeds, utils.WebSeedsFlag.Name, utils.WebSeedsFlag.Value, utils.WebSeedsFlag.Usage)
	rootCmd.Flags().StringVar(&natSetting, "nat", utils.NATFlag.Value, utils.NATFlag.Usage)
	rootCmd.Flags().StringVar(&downloaderApiAddr, "downloader.api.addr", "127.0.0.1:9093", "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface")
	rootCmd.Flags().StringVar(&downloadRateStr, "torrent.download.rate", utils.TorrentDownloadRateFlag.Value, utils.TorrentDownloadRateFlag.Usage)
	rootCmd.Flags().StringVar(&uploadRateStr, "torrent.upload.rate", utils.TorrentUploadRateFlag.Value, utils.TorrentUploadRateFlag.Usage)
	rootCmd.Flags().IntVar(&torrentVerbosity, "torrent.verbosity", utils.TorrentVerbosityFlag.Value, utils.TorrentVerbosityFlag.Usage)
	rootCmd.Flags().IntVar(&torrentPort, "torrent.port", utils.TorrentPortFlag.Value, utils.TorrentPortFlag.Usage)
	rootCmd.Flags().IntVar(&torrentMaxPeers, "torrent.maxpeers", utils.TorrentMaxPeersFlag.Value, utils.TorrentMaxPeersFlag.Usage)
	rootCmd.Flags().IntVar(&torrentConnsPerFile, "torrent.conns.perfile", utils.TorrentConnsPerFileFlag.Value, utils.TorrentConnsPerFileFlag.Usage)
	// Deprecated.
	rootCmd.Flags().IntVar(&torrentDownloadSlots, "torrent.download.slots", utils.TorrentDownloadSlotsFlag.Value, utils.TorrentDownloadSlotsFlag.Usage)
	rootCmd.Flags().StringVar(&staticPeersStr, utils.TorrentStaticPeersFlag.Name, utils.TorrentStaticPeersFlag.Value, utils.TorrentStaticPeersFlag.Usage)
	rootCmd.Flags().BoolVar(&disableIPV6, "downloader.disable.ipv6", utils.DisableIPV6.Value, utils.DisableIPV6.Usage)
	rootCmd.Flags().BoolVar(&disableIPV4, "downloader.disable.ipv4", utils.DisableIPV4.Value, utils.DisableIPV6.Usage)
	rootCmd.Flags().BoolVar(&seedbox, "seedbox", false, "Turns downloader into independent (doesn't need Erigon) software which discover/download/seed new files - useful for Erigon network, and can work on very cheap hardware. It will: 1) download .torrent from webseed 2) download new files after upgrade 3) we planing add discovery of new files soon")
	rootCmd.Flags().BoolVar(&dbWritemap, utils.DbWriteMapFlag.Name, utils.DbWriteMapFlag.Value, utils.DbWriteMapFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&verify, "verify", false, utils.DownloaderVerifyFlag.Usage)
	rootCmd.PersistentFlags().StringVar(&_verifyFiles, "verify.files", "", "Limit list of files to verify")
	rootCmd.PersistentFlags().BoolVar(&verifyFailfast, "verify.failfast", false, "Stop on first found error. Report it and exit")

	withDataDir(createTorrent)
	withFile(createTorrent)
	withChainFlag(createTorrent)
	rootCmd.AddCommand(createTorrent)
	createTorrent.Flags().BoolVar(&all, "all", true, "Produce all possible .torrent files")

	rootCmd.AddCommand(torrentCat)
	rootCmd.AddCommand(torrentMagnet)

	withDataDir(torrentClean)
	rootCmd.AddCommand(torrentClean)

	withDataDir(manifestCmd)
	withChainFlag(manifestCmd)
	rootCmd.AddCommand(manifestCmd)
	manifestCmd.Flags().BoolVar(&all, "all", true, "Produce all possible .torrent files")

	manifestVerifyCmd.Flags().StringVar(&webseeds, utils.WebSeedsFlag.Name, utils.WebSeedsFlag.Value, utils.WebSeedsFlag.Usage)
	manifestVerifyCmd.PersistentFlags().BoolVar(&verifyFailfast, "verify.failfast", false, "Stop on first found error. Report it and exit")
	withChainFlag(manifestVerifyCmd)
	rootCmd.AddCommand(manifestVerifyCmd)

	withDataDir(printTorrentHashes)
	withChainFlag(printTorrentHashes)
	printTorrentHashes.Flags().BoolVar(&all, "all", true, "Produce all possible .torrent files")
	printTorrentHashes.PersistentFlags().BoolVar(&forceRebuild, "rebuild", false, "Force re-create .torrent files")
	printTorrentHashes.Flags().StringVar(&targetFile, "targetfile", "", "write output to file")
	if err := printTorrentHashes.MarkFlagFilename("targetfile"); err != nil {
		panic(err)
	}
	rootCmd.AddCommand(printTorrentHashes)

}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	must(cmd.MarkFlagRequired(utils.DataDirFlag.Name))
	must(cmd.MarkFlagDirname(utils.DataDirFlag.Name))
}
func withChainFlag(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chain, utils.ChainFlag.Name, utils.ChainFlag.Value, utils.ChainFlag.Usage)
	must(cmd.MarkFlagRequired(utils.ChainFlag.Name))
}
func withFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&filePath, "file", "", "")
	if err := cmd.MarkFlagFilename(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

var logger log.Logger
var rootCmd = &cobra.Command{
	Use:     "",
	Short:   "snapshot downloader",
	Example: "go run ./cmd/downloader --datadir <your_datadir> --downloader.api.addr 127.0.0.1:9093",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cmd.Name() != "torrent_cat" {
			logger = debug.SetupCobra(cmd, "downloader")
			logger.Info("Build info", "git_branch", version.GitBranch, "git_tag", version.GitTag, "git_commit", version.GitCommit)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := Downloader(cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func Downloader(ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}
	if err := checkChainName(ctx, dirs, chain); err != nil {
		return err
	}
	torrentLogLevel, err := downloadercfg.Int2LogLevel(torrentVerbosity)
	if err != nil {
		return err
	}

	downloadRate, err := utils.GetStringFlagRateLimit(downloadRateStr)
	if err != nil {
		return err
	}
	uploadRate, err := utils.GetStringFlagRateLimit(uploadRateStr)
	if err != nil {
		return err
	}

	logger.Info(
		"[snapshots] cli flags",
		"chain", chain,
		"addr", downloaderApiAddr,
		"datadir", dirs.DataDir,
		"ipv6-enabled", !disableIPV6,
		"ipv4-enabled", !disableIPV4,
		"download.rate", downloadRateStr,
		"upload.rate", uploadRateStr,
		"webseed", webseeds,
	)

	version := "erigon: " + version.VersionWithCommit(version.GitCommit)

	webseedsList := common.CliString2Array(webseeds)
	if known, ok := snapcfg.KnownWebseeds[chain]; ok {
		webseedsList = append(webseedsList, known...)
	}
	if seedbox {
		_, err = downloadercfg.LoadSnapshotsHashes(ctx, dirs, chain)
		if err != nil {
			return err
		}
	}
	cfg, err := downloadercfg.New(
		ctx,
		dirs,
		version,
		torrentLogLevel,
		torrentPort,
		torrentConnsPerFile,
		webseedsList,
		chain,
		dbWritemap,
		downloadercfg.NewCfgOpts{
			DownloadRateLimit: downloadRate.TorrentRateLimit(),
			UploadRateLimit:   uploadRate.TorrentRateLimit(),
		},
	)
	if err != nil {
		return err
	}

	cfg.ClientConfig.PieceHashersPerTorrent = dbg.EnvInt("DL_HASHERS", runtime.NumCPU())
	cfg.ClientConfig.DisableIPv6 = disableIPV6
	cfg.ClientConfig.DisableIPv4 = disableIPV4

	natif, err := nat.Parse(natSetting)
	if err != nil {
		return fmt.Errorf("invalid nat option %s: %w", natSetting, err)
	}
	downloadernat.DoNat(natif, cfg.ClientConfig, logger)

	// Called manually to ensure all torrents are present before verification.
	cfg.AddTorrentsFromDisk = false
	manualDataVerification := verify || verifyFailfast || len(verifyFiles) > 0
	cfg.ManualDataVerification = manualDataVerification

	d, err := downloader.New(ctx, cfg, logger, log.LvlInfo)
	if err != nil {
		return err
	}
	defer d.Close()
	logger.Info("[snapshots] Start bittorrent server", "my_peer_id", fmt.Sprintf("%x", d.TorrentClient().PeerID()))

	d.HandleTorrentClientStatus(nil)

	err = d.AddTorrentsFromDisk(ctx)
	if err != nil {
		return fmt.Errorf("adding torrents from disk: %w", err)
	}

	// I think we could use DisableInitialPieceVerification to get the behaviour we want here: One
	// hash, and fail if it's in the verify files list or we have fail fast on.

	if len(_verifyFiles) > 0 {
		verifyFiles = strings.Split(_verifyFiles, ",")
	}
	if manualDataVerification { // remove and create .torrent files (will re-read all snapshots)
		if err = d.VerifyData(ctx, verifyFiles); err != nil {
			return err
		}
	}

	// This only works if Cfg.ManualDataVerification is held by reference by the Downloader. The
	// alternative is to pass the value through AddTorrentsFromDisk, do it per Torrent ourselves, or
	// defer all hashing to the torrent Client in the Downloader and wait for it to complete.
	cfg.ManualDataVerification = false

	bittorrentServer, err := downloader.NewGrpcServer(d)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}

	// I'm kinda curious... but it was false before.
	d.MainLoopInBackground(true)
	if seedbox {
		var downloadItems []*proto_downloader.AddItem
		snapCfg, _ := snapcfg.KnownCfg(chain)
		for _, it := range snapCfg.Preverified.Items {
			downloadItems = append(downloadItems, &proto_downloader.AddItem{
				Path:        it.Name,
				TorrentHash: downloadergrpc.String2Proto(it.Hash),
			})
		}
		if _, err := bittorrentServer.Add(ctx, &proto_downloader.AddRequest{Items: downloadItems}); err != nil {
			return err
		}
	}

	grpcServer, err := StartGrpc(bittorrentServer, downloaderApiAddr, nil /* transportCredentials */, logger)
	if err != nil {
		return err
	}
	defer grpcServer.GracefulStop()

	<-ctx.Done()
	return nil
}

var createTorrent = &cobra.Command{
	Use:     "torrent_create",
	Example: "go run ./cmd/downloader torrent_create --datadir=<your_datadir> --file=<relative_file_path> ",
	RunE: func(cmd *cobra.Command, args []string) error {
		dirs := datadir.New(datadirCli)
		if err := checkChainName(cmd.Context(), dirs, chain); err != nil {
			return err
		}
		createdAmount, err := downloader.BuildTorrentFilesIfNeed(cmd.Context(), dirs, downloader.NewAtomicTorrentFS(dirs.Snap), chain, nil, all)
		if err != nil {
			return err
		}
		log.Info("created .torrent files", "amount", createdAmount)
		return nil
	},
}

var printTorrentHashes = &cobra.Command{
	Use:     "torrent_hashes",
	Example: "go run ./cmd/downloader torrent_hashes --datadir <your_datadir>",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "downloader")
		if err := doPrintTorrentHashes(cmd.Context(), logger); err != nil {
			log.Error(err.Error())
		}
		return nil
	},
}

var manifestCmd = &cobra.Command{
	Use:     "manifest",
	Example: "go run ./cmd/downloader manifest --datadir <your_datadir>",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "downloader")
		if err := manifest(cmd.Context(), logger); err != nil {
			log.Error(err.Error())
		}
		return nil
	},
}

var manifestVerifyCmd = &cobra.Command{
	Use:     "manifest_verify",
	Aliases: []string{"manifest-verify"},
	Example: "go run ./cmd/downloader manifest_verify --chain <chain> [--webseed 'a','b','c']",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "downloader")
		if err := manifestVerify(cmd.Context(), logger); err != nil {
			log.Error(err.Error())
			os.Exit(1) // to mark CI as failed
		}
		return nil
	},
}

var torrentCat = &cobra.Command{
	Use:     "torrent_cat",
	Example: "go run ./cmd/downloader torrent_cat <path_to_torrent_file>",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("please pass .torrent file path by first argument")
		}
		fPath := args[0]
		mi, err := metainfo.LoadFromFile(fPath)
		if err != nil {
			return fmt.Errorf("LoadFromFile: %w, file=%s", err, fPath)
		}
		var ms map[string]any
		err = mapstructure.Decode(mi, &ms)
		if err != nil {
			return fmt.Errorf("decoding metainfo into map: %w", err)
		}
		fmt.Printf("InfoHash = '%x'\n", mi.HashInfoBytes())
		delete(ms, "InfoBytes")
		bytes, err := toml.Marshal(ms)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(bytes)
		return err
	},
}
var torrentClean = &cobra.Command{
	Use:     "torrent_clean",
	Short:   "RemoveFile all .torrent files from datadir directory",
	Example: "go run ./cmd/downloader torrent_clean --datadir=<datadir>",
	RunE: func(cmd *cobra.Command, args []string) error {
		dirs := datadir.New(datadirCli)

		logger.Info("[snapshots.webseed] processing local file etags")
		removedTorrents := 0
		walker := func(path string, de fs.DirEntry, err error) error {
			if err != nil || de.IsDir() {
				if err != nil {
					logger.Warn("[snapshots.torrent] walk and cleanup", "err", err, "path", path)
				}
				return nil //nolint
			}

			if !strings.HasSuffix(de.Name(), ".torrent") || strings.HasPrefix(de.Name(), ".") {
				return nil
			}
			err = dir.RemoveFile(filepath.Join(dirs.Snap, path))
			if err != nil {
				logger.Warn("[snapshots.torrent] remove", "err", err, "path", path)
				return err
			}
			removedTorrents++
			return nil
		}

		sfs := os.DirFS(dirs.Snap)
		if err := fs.WalkDir(sfs, ".", walker); err != nil {
			return err
		}
		logger.Info("[snapshots.torrent] cleanup finished", "count", removedTorrents)
		return nil
	},
}

var torrentMagnet = &cobra.Command{
	Use:     "torrent_magnet",
	Example: "go run ./cmd/downloader torrent_magnet <path_to_torrent_file>",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("please pass .torrent file path by first argument")
		}
		fPath := args[0]
		mi, err := metainfo.LoadFromFile(fPath)
		if err != nil {
			return fmt.Errorf("LoadFromFile: %w, file=%s", err, fPath)
		}
		fmt.Printf("%s\n", mi.Magnet(nil, nil).String())
		return nil
	},
}

func manifestVerify(ctx context.Context, logger log.Logger) error {
	webseedsList := common.CliString2Array(webseeds)
	if len(webseedsList) == 0 { // fallback to default if exact list not passed
		if known, ok := snapcfg.KnownWebseeds[chain]; ok {
			for _, s := range known {
				//TODO: enable validation of this buckets also. skipping to make CI useful.k
				if strings.Contains(s, "erigon2-v2") {
					continue
				}
				webseedsList = append(webseedsList, s)
			}
		}
	}

	webseedUrlsOrFiles := webseedsList
	webseedHttpProviders := make([]*url.URL, 0, len(webseedUrlsOrFiles))
	webseedFileProviders := make([]string, 0, len(webseedUrlsOrFiles))
	for _, webseed := range webseedUrlsOrFiles {
		if !strings.HasPrefix(webseed, "v") { // has marker v1/v2/...
			uri, err := url.ParseRequestURI(webseed)
			if err != nil {
				exists, existsErr := dir.FileExist(webseed)
				if existsErr != nil {
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
	if len(webseedFileProviders) > 0 {
		logger.Warn("file providers are not supported yet", "fileProviders", webseedFileProviders)
	}

	wseed := downloader.NewWebSeeds(webseedHttpProviders, log.LvlDebug, logger)
	return wseed.VerifyManifestedBuckets(ctx, verifyFailfast)
}

func manifest(ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)

	files, err := downloader.SeedableFiles(dirs, chain, true)
	if err != nil {
		return err
	}

	extList := []string{
		".torrent",
		//".seg", ".idx", // e2
		//".kv", ".kvi", ".bt", ".kvei", // e3 domain
		//".v", ".vi", //e3 hist
		//".ef", ".efi", //e3 idx
		".txt", //salt-state.txt, salt-blocks.txt, manifest.txt
	}
	l, _ := dir.ListFiles(dirs.Snap, extList...)
	for _, fPath := range l {
		_, fName := filepath.Split(fPath)
		files = append(files, fName)
	}
	l, _ = dir.ListFiles(dirs.SnapDomain, extList...)
	for _, fPath := range l {
		_, fName := filepath.Split(fPath)
		files = append(files, "domain/"+fName)
	}
	l, _ = dir.ListFiles(dirs.SnapHistory, extList...)
	for _, fPath := range l {
		_, fName := filepath.Split(fPath)
		files = append(files, "history/"+fName)
	}
	l, _ = dir.ListFiles(dirs.SnapIdx, extList...)
	for _, fPath := range l {
		_, fName := filepath.Split(fPath)
		files = append(files, "idx/"+fName)
	}

	sort.Strings(files)
	for _, f := range files {
		fmt.Printf("%s\n", f)
	}
	return nil
}

func doPrintTorrentHashes(ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}

	tf := downloader.NewAtomicTorrentFS(dirs.Snap)

	if forceRebuild { // remove and create .torrent files (will re-read all snapshots)
		//removePieceCompletionStorage(snapDir)
		files, err := downloader.AllTorrentPaths(dirs)
		if err != nil {
			return err
		}
		for _, filePath := range files {
			if err := dir.RemoveFile(filePath); err != nil {
				return err
			}
		}
		createdAmount, err := downloader.BuildTorrentFilesIfNeed(ctx, dirs, tf, chain, nil, all)
		if err != nil {
			return fmt.Errorf("BuildTorrentFilesIfNeed: %w", err)
		}
		log.Info("created .torrent files", "amount", createdAmount)
	}

	res := map[string]string{}
	torrents, err := downloader.AllTorrentSpecs(dirs, tf)
	if err != nil {
		return err
	}

	for _, t := range torrents {
		res[t.DisplayName] = t.InfoHash.String()
	}
	serialized, err := toml.Marshal(res)
	if err != nil {
		return err
	}

	if targetFile == "" {
		fmt.Printf("%s\n", serialized)
		return nil
	}

	oldContent, err := os.ReadFile(targetFile)
	if err != nil {
		return err
	}
	oldLines := map[string]string{}
	if err := toml.Unmarshal(oldContent, &oldLines); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	if len(oldLines) >= len(res) {
		logger.Info("amount of lines in target file is equal or greater than amount of lines in snapshot dir", "old", len(oldLines), "new", len(res))
		return nil
	}
	if err := os.WriteFile(targetFile, serialized, 0644); err != nil { // nolint
		return err
	}
	return nil
}

func StartGrpc(snServer *downloader.GrpcServer, addr string, creds *credentials.TransportCredentials, logger log.Logger) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("could not create listener: %w, addr=%s", err, addr)
	}

	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	//if metrics.Enabled {
	//	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}

	opts := []grpc.ServerOption{
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	if creds == nil {
		// no specific opts
	} else {
		opts = append(opts, grpc.Creds(*creds))
	}
	grpcServer := grpc.NewServer(opts...)
	reflection.Register(grpcServer) // Register reflection service on gRPC server.
	if snServer != nil {
		proto_downloader.RegisterDownloaderServer(grpcServer, snServer)
	}

	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		defer healthServer.Shutdown()
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server stop", "err", err)
		}
	}()
	logger.Info("Started gRPC server", "on", addr)
	return grpcServer, nil
}

func checkChainName(ctx context.Context, dirs datadir.Dirs, chainName string) error {
	exists, err := dir.FileExist(filepath.Join(dirs.Chaindata, "mdbx.dat"))
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	db, err := mdbx.New(kv.ChainDB, log.New()).
		Path(dirs.Chaindata).
		Accede(true).
		Open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()

	if cc := tool.ChainConfigFromDB(db); cc != nil {
		spc, err := chainspec.ChainSpecByName(chainName)
		if err != nil {
			return fmt.Errorf("unknown chain: %s", chainName)
		}
		if spc.Config.ChainID.Uint64() != cc.ChainID.Uint64() {
			advice := fmt.Sprintf("\nTo change to '%s', remove %s %s\nAnd then start over with --chain=%s", chainName, dirs.Chaindata, filepath.Join(dirs.Snap, "preverified.toml"), chainName)
			return fmt.Errorf("datadir already was configured with --chain=%s"+advice, cc.ChainName)
		}
	}
	return nil
}
