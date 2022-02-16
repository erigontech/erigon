package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/common"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/internal/debug"
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
	datadir                        string
	asJson                         bool
	forceRebuild                   bool
	forceVerify                    bool
	downloaderApiAddr              string
	torrentVerbosity               string
	downloadRateStr, uploadRateStr string
	torrentPort                    int
)

func init() {
	flags := append(debug.Flags, utils.MetricFlags...)
	utils.CobraFlags(rootCmd, flags)

	withDatadir(rootCmd)

	rootCmd.Flags().StringVar(&downloaderApiAddr, "downloader.api.addr", "127.0.0.1:9093", "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface")
	rootCmd.Flags().StringVar(&torrentVerbosity, "torrent.verbosity", lg.Warning.LogString(), "DEBUG | INFO | WARN | ERROR")
	rootCmd.Flags().StringVar(&downloadRateStr, "torrent.download.rate", "8mb", "bytes per second, example: 32mb")
	rootCmd.Flags().StringVar(&uploadRateStr, "torrent.upload.rate", "8mb", "bytes per second, example: 32mb")
	rootCmd.Flags().IntVar(&torrentPort, "torrent.port", 42069, "port to listen and serve BitTorrent protocol")

	withDatadir(printTorrentHashes)
	printTorrentHashes.PersistentFlags().BoolVar(&asJson, "json", false, "Print in json format (default: toml)")
	printTorrentHashes.PersistentFlags().BoolVar(&forceRebuild, "rebuild", false, "Force re-create .torrent files")
	printTorrentHashes.PersistentFlags().BoolVar(&forceVerify, "verify", false, "Force verify data files if have .torrent files")

	rootCmd.AddCommand(printTorrentHashes)
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := cmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
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
	snapshotDir := filepath.Join(datadir, "snapshots")
	common.MustExist(snapshotDir)
	torrentLogLevel, ok := torrentcfg.String2LogLevel[torrentVerbosity]
	if !ok {
		panic(fmt.Errorf("unexpected torrent.verbosity level: %s", torrentVerbosity))
	}

	var downloadRate, uploadRate datasize.ByteSize
	if err := downloadRate.UnmarshalText([]byte(downloadRateStr)); err != nil {
		return err
	}
	if err := uploadRate.UnmarshalText([]byte(uploadRateStr)); err != nil {
		return err
	}

	log.Info("Run snapshot downloader", "addr", downloaderApiAddr, "datadir", datadir, "download.rate", downloadRate.String(), "upload.rate", uploadRate.String())

	cfg, pieceCompletion, err := torrentcfg.New(snapshotDir, torrentLogLevel, downloadRate, uploadRate, torrentPort)
	if err != nil {
		return fmt.Errorf("New: %w", err)
	}
	defer pieceCompletion.Close()

	protocols, err := downloader.New(cfg, snapshotDir)
	if err != nil {
		return err
	}
	defer protocols.Close()
	log.Info("[torrent] Start", "my peerID", fmt.Sprintf("%x", protocols.TorrentClient.PeerID()))
	if err = downloader.CreateTorrentFilesAndAdd(ctx, snapshotDir, protocols.TorrentClient); err != nil {
		return fmt.Errorf("CreateTorrentFilesAndAdd: %w", err)
	}

	go downloader.LoggingLoop(ctx, protocols.TorrentClient)

	bittorrentServer, err := downloader.NewGrpcServer(protocols.DB, protocols, snapshotDir, true)
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
		snapshotDir := filepath.Join(datadir, "snapshots")
		ctx := cmd.Context()

		if forceVerify { // remove and create .torrent files (will re-read all snapshots)
			return downloader.VerifyDtaFiles(ctx, snapshotDir)
		}

		if forceRebuild { // remove and create .torrent files (will re-read all snapshots)
			removeChunksStorage(snapshotDir)

			files, err := downloader.AllTorrentPaths(snapshotDir)
			if err != nil {
				return err
			}
			for _, filePath := range files {
				if err := os.Remove(filePath); err != nil {
					return err
				}
			}
			if err := downloader.BuildTorrentFilesIfNeed(ctx, snapshotDir); err != nil {
				return err
			}
		}

		res := map[string]string{}
		files, err := downloader.AllTorrentPaths(snapshotDir)
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
		var serialized []byte
		if asJson {
			serialized, err = json.Marshal(res)
			if err != nil {
				return err
			}
		} else {
			serialized, err = toml.Marshal(res)
			if err != nil {
				return err
			}
		}
		fmt.Printf("%s\n", serialized)
		return nil
	},
}

func removeChunksStorage(snapshotDir string) {
	_ = os.RemoveAll(filepath.Join(snapshotDir, ".torrent.db"))
	_ = os.RemoveAll(filepath.Join(snapshotDir, ".torrent.bolt.db"))
	_ = os.RemoveAll(filepath.Join(snapshotDir, ".torrent.db-shm"))
	_ = os.RemoveAll(filepath.Join(snapshotDir, ".torrent.db-wal"))
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
