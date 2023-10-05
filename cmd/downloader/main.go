package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	mdbx2 "github.com/erigontech/mdbx-go/mdbx"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader"
	downloadercfg2 "github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/ledgerwatch/erigon/cmd/downloader/downloadernat"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
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
	forceVerify                    bool
	downloaderApiAddr              string
	natSetting                     string
	torrentVerbosity               int
	downloadRateStr, uploadRateStr string
	torrentDownloadSlots           int
	staticPeersStr                 string
	torrentPort                    int
	torrentMaxPeers                int
	torrentConnsPerFile            int
	targetFile                     string
	disableIPV6                    bool
	disableIPV4                    bool
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)

	withDataDir(rootCmd)
	rootCmd.Flags().StringVar(&chain, utils.ChainFlag.Name, utils.ChainFlag.Value, utils.ChainFlag.Usage)
	rootCmd.Flags().StringVar(&webseeds, utils.WebSeedsFlag.Name, utils.WebSeedsFlag.Value, utils.WebSeedsFlag.Usage)
	rootCmd.Flags().StringVar(&natSetting, "nat", utils.NATFlag.Value, utils.NATFlag.Usage)
	rootCmd.Flags().StringVar(&downloaderApiAddr, "downloader.api.addr", "127.0.0.1:9093", "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface")
	rootCmd.Flags().StringVar(&downloadRateStr, "torrent.download.rate", utils.TorrentDownloadRateFlag.Value, utils.TorrentDownloadRateFlag.Usage)
	rootCmd.Flags().StringVar(&uploadRateStr, "torrent.upload.rate", utils.TorrentUploadRateFlag.Value, utils.TorrentUploadRateFlag.Usage)
	rootCmd.Flags().IntVar(&torrentVerbosity, "torrent.verbosity", utils.TorrentVerbosityFlag.Value, utils.TorrentVerbosityFlag.Usage)
	rootCmd.Flags().IntVar(&torrentPort, "torrent.port", utils.TorrentPortFlag.Value, utils.TorrentPortFlag.Usage)
	rootCmd.Flags().IntVar(&torrentMaxPeers, "torrent.maxpeers", utils.TorrentMaxPeersFlag.Value, utils.TorrentMaxPeersFlag.Usage)
	rootCmd.Flags().IntVar(&torrentConnsPerFile, "torrent.conns.perfile", utils.TorrentConnsPerFileFlag.Value, utils.TorrentConnsPerFileFlag.Usage)
	rootCmd.Flags().IntVar(&torrentDownloadSlots, "torrent.download.slots", utils.TorrentDownloadSlotsFlag.Value, utils.TorrentDownloadSlotsFlag.Usage)
	rootCmd.Flags().StringVar(&staticPeersStr, utils.TorrentStaticPeersFlag.Name, utils.TorrentStaticPeersFlag.Value, utils.TorrentStaticPeersFlag.Usage)
	rootCmd.Flags().BoolVar(&disableIPV6, "downloader.disable.ipv6", utils.DisableIPV6.Value, utils.DisableIPV6.Usage)
	rootCmd.Flags().BoolVar(&disableIPV4, "downloader.disable.ipv4", utils.DisableIPV4.Value, utils.DisableIPV6.Usage)
	rootCmd.PersistentFlags().BoolVar(&forceVerify, "verify", false, "Force verify data files if have .torrent files")

	withDataDir(createTorrent)
	withFile(createTorrent)

	withDataDir(printTorrentHashes)
	printTorrentHashes.PersistentFlags().BoolVar(&forceRebuild, "rebuild", false, "Force re-create .torrent files")
	printTorrentHashes.Flags().StringVar(&targetFile, "targetfile", "", "write output to file")
	if err := printTorrentHashes.MarkFlagFilename("targetfile"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(createTorrent)
	rootCmd.AddCommand(printTorrentHashes)
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := cmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}
}
func withFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&filePath, "file", "", "")
	if err := cmd.MarkFlagFilename(utils.DataDirFlag.Name); err != nil {
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
		logger = debug.SetupCobra(cmd, "downloader")
		logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)
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
	if err := checkChainName(dirs, chain); err != nil {
		return err
	}
	torrentLogLevel, _, err := downloadercfg2.Int2LogLevel(torrentVerbosity)
	if err != nil {
		return err
	}

	var downloadRate, uploadRate datasize.ByteSize
	if err := downloadRate.UnmarshalText([]byte(downloadRateStr)); err != nil {
		return err
	}
	if err := uploadRate.UnmarshalText([]byte(uploadRateStr)); err != nil {
		return err
	}

	logger.Info("[snapshots] cli flags", "chain", chain, "addr", downloaderApiAddr, "datadir", dirs.DataDir, "ipv6-enabled", !disableIPV6, "ipv4-enabled", !disableIPV4, "download.rate", downloadRate.String(), "upload.rate", uploadRate.String(), "webseed", webseeds)
	staticPeers := common.CliString2Array(staticPeersStr)

	version := "erigon: " + params.VersionWithCommit(params.GitCommit)
	cfg, err := downloadercfg2.New(dirs, version, torrentLogLevel, downloadRate, uploadRate, torrentPort, torrentConnsPerFile, torrentDownloadSlots, staticPeers, webseeds)
	if err != nil {
		return err
	}

	cfg.ClientConfig.PieceHashersPerTorrent = runtime.NumCPU() * 4
	cfg.ClientConfig.DisableIPv6 = disableIPV6
	cfg.ClientConfig.DisableIPv4 = disableIPV4

	natif, err := nat.Parse(natSetting)
	if err != nil {
		return fmt.Errorf("invalid nat option %s: %w", natSetting, err)
	}
	downloadernat.DoNat(natif, cfg.ClientConfig, logger)

	d, err := downloader.New(ctx, cfg, logger, log.LvlInfo)
	if err != nil {
		return err
	}
	defer d.Close()
	logger.Info("[snapshots] Start bittorrent server", "my_peer_id", fmt.Sprintf("%x", d.TorrentClient().PeerID()))

	if forceVerify { // remove and create .torrent files (will re-read all snapshots)
		if err = d.VerifyData(ctx); err != nil {
			return err
		}
	}

	d.MainLoopInBackground(false)

	if err := addPreConfiguredHashes(ctx, d); err != nil {
		return err
	}

	bittorrentServer, err := downloader.NewGrpcServer(d)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
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
	Example: "go run ./cmd/downloader torrent_create --datadir=<your_datadir> --file=<relative_file_path>",
	RunE: func(cmd *cobra.Command, args []string) error {
		//logger := debug.SetupCobra(cmd, "integration")
		dirs := datadir.New(datadirCli)
		err := downloader.BuildTorrentFilesIfNeed(cmd.Context(), dirs)
		if err != nil {
			return err
		}
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

func doPrintTorrentHashes(ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}

	if forceRebuild { // remove and create .torrent files (will re-read all snapshots)
		//removePieceCompletionStorage(snapDir)
		files, err := downloader.AllTorrentPaths(dirs)
		if err != nil {
			return err
		}
		for _, filePath := range files {
			if err := os.Remove(filePath); err != nil {
				return err
			}
		}
		if err := downloader.BuildTorrentFilesIfNeed(ctx, dirs); err != nil {
			return fmt.Errorf("BuildTorrentFilesIfNeed: %w", err)
		}
	}

	res := map[string]string{}
	torrents, err := downloader.AllTorrentSpecs(dirs)
	if err != nil {
		return err
	}
	for _, t := range torrents {
		// we don't release commitment history in this time. let's skip it here.
		if strings.HasPrefix(t.DisplayName, "history/commitment") {
			continue
		}
		if strings.HasPrefix(t.DisplayName, "idx/commitment") {
			continue
		}
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

// Add pre-configured
func addPreConfiguredHashes(ctx context.Context, d *downloader.Downloader) error {
	for _, it := range snapcfg.KnownCfg(chain, nil, nil).Preverified {
		if err := d.AddInfoHashAsMagnetLink(ctx, snaptype.Hex2InfoHash(it.Hash), it.Name); err != nil {
			return err
		}
	}
	return nil
}

func checkChainName(dirs datadir.Dirs, chainName string) error {
	if !dir.FileExist(filepath.Join(dirs.Chaindata, "mdbx.dat")) {
		return nil
	}
	db := mdbx.NewMDBX(log.New()).
		Path(dirs.Chaindata).Label(kv.ChainDB).
		Flags(func(flags uint) uint { return flags | mdbx2.Accede }).
		MustOpen()
	defer db.Close()
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		cc := tool.ChainConfig(tx)
		if cc != nil && cc.ChainName != chainName {
			return fmt.Errorf("datadir already was configured with --chain=%s. can't change to '%s'", cc.ChainName, chainName)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
