package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/c2h5oh/datasize"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/erigon-lib/common"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/cmd/hack/tool"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
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
	datadir                       string
	seeding                       bool
	asJson                        bool
	forceRebuild                  bool
	downloaderApiAddr             string
	torrentVerbosity              string
	downloadRateStr, uploadRteStr string
	torrentPort                   int
)

func init() {
	flags := append(debug.Flags, utils.MetricFlags...)
	utils.CobraFlags(rootCmd, flags)

	withDatadir(rootCmd)

	rootCmd.PersistentFlags().BoolVar(&seeding, "seeding", true, "Seed snapshots")
	rootCmd.Flags().StringVar(&downloaderApiAddr, "downloader.api.addr", "127.0.0.1:9093", "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface")
	rootCmd.Flags().StringVar(&torrentVerbosity, "torrent.verbosity", lg.Warning.LogString(), "DEBUG | INFO | WARN | ERROR")
	rootCmd.Flags().StringVar(&downloadRateStr, "download.rate", "8mb", "bytes per second, example: 32mb")
	rootCmd.Flags().StringVar(&uploadRteStr, "upload.rate", "8mb", "bytes per second, example: 32mb")
	rootCmd.Flags().IntVar(&torrentPort, "torrent.port", 42069, "port to listen and serve BitTorrent protocol")

	withDatadir(printInfoHashes)
	printInfoHashes.PersistentFlags().BoolVar(&asJson, "json", false, "Print in json format (default: toml)")
	printInfoHashes.PersistentFlags().BoolVar(&forceRebuild, "rebuild", false, "Force re-create .torrent files")

	rootCmd.AddCommand(printInfoHashes)
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
		if err := Downloader(cmd.Context(), cmd); err != nil {
			log.Error("Downloader", "err", err)
			return nil
		}
		return nil
	},
}

func Downloader(ctx context.Context, cmd *cobra.Command) error {
	snapshotsDir := path.Join(datadir, "snapshots")
	torrentLogLevel, ok := downloader.String2LogLevel[torrentVerbosity]
	if !ok {
		panic(fmt.Errorf("unexpected torrent.verbosity level: %s", torrentVerbosity))
	}

	var downloadRate, uploadRate datasize.ByteSize
	if err := downloadRate.UnmarshalText([]byte(downloadRateStr)); err != nil {
		return err
	}
	if err := uploadRate.UnmarshalText([]byte(uploadRteStr)); err != nil {
		return err
	}

	log.Info("Run snapshot downloader", "addr", downloaderApiAddr, "datadir", datadir, "seeding", seeding, "download.rate", downloadRate.String(), "upload.rate", uploadRate.String())
	common.MustExist(snapshotsDir)

	db := mdbx.MustOpen(snapshotsDir + "/db")
	var t *downloader.Client
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		peerID, err := tx.GetOne(kv.BittorrentInfo, []byte(kv.BittorrentPeerID))
		if err != nil {
			return fmt.Errorf("get peer id: %w", err)
		}

		cfg, err := downloader.TorrentConfig(snapshotsDir, seeding, string(peerID), torrentLogLevel, downloadRate, uploadRate, torrentPort)
		if err != nil {
			return fmt.Errorf("TorrentConfig: %w", err)
		}
		t, err = downloader.New(cfg)
		if err != nil {
			return err
		}
		if len(peerID) == 0 {
			err = t.SavePeerID(tx)
			if err != nil {
				return fmt.Errorf("save peer id: %w", err)
			}
		}
		log.Info(fmt.Sprintf("Seeding: %t, my peerID: %x", cfg.Seed, t.Cli.PeerID()))
		return nil
	}); err != nil {
		return err
	}
	defer t.Close()

	bittorrentServer, err := downloader.NewServer(db, t, snapshotsDir)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}

	err = downloader.CreateTorrentFilesAndAdd(ctx, snapshotsDir, t.Cli)
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}

	if downloader.ASSERT {
		var cc *params.ChainConfig
		{
			chaindataDir := path.Join(datadir, "chaindata")
			common.MustExist(chaindataDir)
			chaindata, err := mdbx.Open(chaindataDir, log.New(), true)
			if err != nil {
				return fmt.Errorf("%w, path: %s", err, chaindataDir)
			}
			cc = tool.ChainConfigFromDB(chaindata)
			chaindata.Close()
		}

		snapshotsCfg := snapshothashes.KnownConfig(cc.ChainName)
		for _, t := range t.Cli.Torrents() {
			expectHashStr := snapshotsCfg.Preverified[t.Info().Name]
			expectHash := metainfo.NewHashFromHex(expectHashStr)
			if t.InfoHash() != expectHash {
				return fmt.Errorf("file %s has unexpected hash %x, expected %x. May help: git submodule update --init --recursive --force", t.Info().Name, t.InfoHash(), expectHash)
			}
		}
	}

	go downloader.MainLoop(ctx, t.Cli)

	grpcServer, err := StartGrpc(bittorrentServer, downloaderApiAddr, nil)
	if err != nil {
		return err
	}
	<-cmd.Context().Done()
	grpcServer.GracefulStop()
	return nil
}

var printInfoHashes = &cobra.Command{
	Use:     "info_hashes",
	Example: "go run ./cmd/downloader info_hashes --datadir <your_datadir>",
	RunE: func(cmd *cobra.Command, args []string) error {
		snapshotDir := path.Join(datadir, "snapshots")
		ctx := cmd.Context()

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

func StartGrpc(snServer *downloader.SNDownloaderServer, addr string, creds *credentials.TransportCredentials) (*grpc.Server, error) {
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
