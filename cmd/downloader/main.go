package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/common"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	datadirCli                     string
	snapdirCli                     string
	forceRebuild                   bool
	forceVerify                    bool
	downloaderApiAddr              string
	natSetting                     string
	torrentVerbosity               int
	downloadRateStr, uploadRateStr string
	torrentDownloadSlots           int
	torrentPort                    int
	torrentMaxPeers                int
	torrentConnsPerFile            int
	targetFile                     string
)

func init() {
	flags := append(debug.Flags, utils.MetricFlags...)
	utils.CobraFlags(rootCmd, flags)

	withDataDir(rootCmd)
	withSnapDir(rootCmd)

	rootCmd.Flags().StringVar(&natSetting, "nat", utils.NATFlag.Value, utils.NATFlag.Usage)
	rootCmd.Flags().StringVar(&downloaderApiAddr, "downloader.api.addr", "127.0.0.1:9093", "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface")
	rootCmd.Flags().StringVar(&downloadRateStr, "torrent.download.rate", utils.TorrentDownloadRateFlag.Value, utils.TorrentDownloadRateFlag.Usage)
	rootCmd.Flags().StringVar(&uploadRateStr, "torrent.upload.rate", utils.TorrentUploadRateFlag.Value, utils.TorrentUploadRateFlag.Usage)
	rootCmd.Flags().IntVar(&torrentVerbosity, "torrent.verbosity", utils.TorrentVerbosityFlag.Value, utils.TorrentVerbosityFlag.Usage)
	rootCmd.Flags().IntVar(&torrentPort, "torrent.port", utils.TorrentPortFlag.Value, utils.TorrentPortFlag.Usage)
	rootCmd.Flags().IntVar(&torrentMaxPeers, "torrent.maxpeers", utils.TorrentMaxPeersFlag.Value, utils.TorrentMaxPeersFlag.Usage)
	rootCmd.Flags().IntVar(&torrentConnsPerFile, "torrent.conns.perfile", utils.TorrentConnsPerFileFlag.Value, utils.TorrentConnsPerFileFlag.Usage)
	rootCmd.Flags().IntVar(&torrentDownloadSlots, "torrent.download.slots", utils.TorrentDownloadSlotsFlag.Value, utils.TorrentDownloadSlotsFlag.Usage)

	withDataDir(printTorrentHashes)
	printTorrentHashes.PersistentFlags().BoolVar(&forceRebuild, "rebuild", false, "Force re-create .torrent files")
	printTorrentHashes.PersistentFlags().BoolVar(&forceVerify, "verify", false, "Force verify data files if have .torrent files")
	printTorrentHashes.Flags().StringVar(&targetFile, "targetfile", "", "write output to file")
	if err := printTorrentHashes.MarkFlagFilename("targetfile"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(printTorrentHashes)
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := cmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}
}

func withSnapDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapdirCli, utils.SnapDirFlag.Name, paths.DefaultSnapDir(), utils.SnapDirFlag.Usage)
	if err := cmd.MarkFlagDirname(utils.SnapDirFlag.Name); err != nil {
		panic(err)
	}
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:     "",
	Short:   "snapshot downloader",
	Example: "go run ./cmd/snapshots --datadir <your_datadir> --downloader.api.addr 127.0.0.1:9093",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := Downloader(cmd.Context()); err != nil {
			log.Error("Downloader", "err", err)
			return nil
		}
		return nil
	},
}

func Downloader(ctx context.Context) error {
	dirs := datadir.New(datadirCli, snapdirCli)
	torrentLogLevel, dbg, err := downloadercfg.Int2LogLevel(torrentVerbosity)
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

	log.Info("Run snapshot downloader", "addr", downloaderApiAddr, "datadir", dirs.DataDir, "download.rate", downloadRate.String(), "upload.rate", uploadRate.String())
	natif, err := nat.Parse(natSetting)
	if err != nil {
		return fmt.Errorf("invalid nat option %s: %w", natSetting, err)
	}

	cfg, err := downloadercfg.New(dirs.Snap, torrentLogLevel, dbg, natif, downloadRate, uploadRate, torrentPort, torrentConnsPerFile, torrentDownloadSlots)
	if err != nil {
		return err
	}

	d, err := downloader.New(cfg)
	if err != nil {
		return err
	}
	defer d.Close()
	log.Info("[torrent] Start", "my peerID", fmt.Sprintf("%x", d.Torrent().PeerID()))
	go downloader.MainLoop(ctx, d, false)

	bittorrentServer, err := downloader.NewGrpcServer(d)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}

	grpcServer, err := StartGrpc(bittorrentServer, downloaderApiAddr, nil)
	if err != nil {
		return err
	}
	defer grpcServer.GracefulStop()

	<-ctx.Done()
	return nil
}

var printTorrentHashes = &cobra.Command{
	Use:     "torrent_hashes",
	Example: "go run ./cmd/downloader torrent_hashes --datadir <your_datadir>",
	RunE: func(cmd *cobra.Command, args []string) error {
		dirs := datadir.New(datadirCli, snapdirCli)
		ctx := cmd.Context()

		if forceVerify { // remove and create .torrent files (will re-read all snapshots)
			return downloader.VerifyDtaFiles(ctx, dirs.Snap)
		}

		if forceRebuild { // remove and create .torrent files (will re-read all snapshots)
			//removePieceCompletionStorage(snapDir)
			files, err := downloader.AllTorrentPaths(dirs.Snap)
			if err != nil {
				return err
			}
			for _, filePath := range files {
				if err := os.Remove(filePath); err != nil {
					return err
				}
			}
			if err := downloader.BuildTorrentFilesIfNeed(ctx, dirs.Snap); err != nil {
				return err
			}
		}

		res := map[string]string{}
		files, err := downloader.AllTorrentPaths(dirs.Snap)
		if err != nil {
			return err
		}
		for _, torrentFilePath := range files {
			mi, err := metainfo.LoadFromFile(torrentFilePath)
			if err != nil {
				return err
			}
			info, err := mi.UnmarshalInfo()
			if err != nil {
				return err
			}
			res[info.Name] = mi.HashInfoBytes().String()
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
			log.Info("amount of lines in target file is equal or greater than amount of lines in snapshot dir", "old", len(oldLines), "new", len(res))
			return nil
		}
		if err := os.WriteFile(targetFile, serialized, 0644); err != nil {
			return err
		}
		return nil
	},
}

//nolint
func removePieceCompletionStorage(snapDir string) {
	_ = os.RemoveAll(filepath.Join(snapDir, "db"))
	_ = os.RemoveAll(filepath.Join(snapDir, ".torrent.db"))
	_ = os.RemoveAll(filepath.Join(snapDir, ".torrent.bolt.db"))
	_ = os.RemoveAll(filepath.Join(snapDir, ".torrent.db-shm"))
	_ = os.RemoveAll(filepath.Join(snapDir, ".torrent.db-wal"))
}

func StartGrpc(snServer *downloader.GrpcServer, addr string, creds *credentials.TransportCredentials) (*grpc.Server, error) {
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
			log.Error("gRPC server stop", "err", err)
		}
	}()
	log.Info("Started gRPC server", "on", addr)
	return grpcServer, nil
}
