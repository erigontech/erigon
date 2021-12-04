package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	proto_snap "github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

func init() {
	flags := append(debug.Flags, utils.MetricFlags...)
	flags = append(flags, Addr, DataDir)
	utils.CobraFlags(rootCmd, flags)

	rootCmd.PersistentFlags().Bool("seeding", true, "Seed snapshots")
}

var (
	Addr = cli.StringFlag{
		Name:  "downloader.api.addr",
		Usage: "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface",
		Value: "127.0.0.1:9093",
	}
	DataDir = cli.StringFlag{
		Name:  utils.DataDirFlag.Name,
		Usage: utils.DataDirFlag.Usage,
		Value: paths.DefaultDataDir(),
	}
)

type Config struct {
	Addr    string
	Dir     string
	Seeding bool
}

func main() {
	ctx, cancel := utils.RootContext()
	defer cancel()

	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}

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
	RunE: runDownloader,
}

func runDownloader(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cfg := &Config{}
	var err error
	cfg.Addr, err = cmd.Flags().GetString(Addr.Name)
	if err != nil {
		return err
	}
	dir, err := cmd.Flags().GetString(DataDir.Name)
	if err != nil {
		return err
	}
	cfg.Dir = path.Join(dir, "snapshots")
	cfg.Seeding, err = cmd.Flags().GetBool("seeding")
	if err != nil {
		return err
	}
	log.Info("Run snapshot downloader", "addr", cfg.Addr, "dir", cfg.Dir, "seeding", cfg.Seeding)

	bittorrentServer, err := snapshotsync.NewServer(cfg.Dir, cfg.Seeding)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}
	log.Info("Load")
	err = bittorrentServer.Load()
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}

	go func() {
		_, err := bittorrentServer.Download(ctx, &proto_snap.DownloadSnapshotRequest{
			NetworkId: params.MainnetChainConfig.ChainID.Uint64(),
			Type:      snapshotsync.GetAvailableSnapshotTypes(params.MainnetChainConfig.ChainID.Uint64()),
		})
		if err != nil {
			log.Error("Download failed", "err", err, "networkID", params.MainnetChainConfig.ChainID.Uint64())
		}
	}()
	go func() {
		for {
			select {
			case <-cmd.Context().Done():
				return
			default:
			}

			snapshots, err := bittorrentServer.Snapshots(ctx, &proto_snap.SnapshotsRequest{
				NetworkId: params.MainnetChainConfig.ChainID.Uint64(),
			})
			if err != nil {
				log.Error("get snapshots", "err", err)
				time.Sleep(time.Minute)
				continue
			}
			stats := bittorrentServer.Stats(context.Background())
			for _, v := range snapshots.Info {
				log.Info("Snapshot "+v.Type.String(), "%", v.Readiness, "peers", stats[v.Type.String()].ConnectedSeeders)
			}
			time.Sleep(time.Minute)
		}
	}()
	grpcServer, err := StartGrpc(bittorrentServer, cfg.Addr, nil)
	if err != nil {
		return err
	}
	<-cmd.Context().Done()
	grpcServer.GracefulStop()

	return nil

}

func StartGrpc(snServer *snapshotsync.SNDownloaderServer, addr string, creds *credentials.TransportCredentials) (*grpc.Server, error) {
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
		proto_snap.RegisterDownloaderServer(grpcServer, snServer)
	}
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}

	go func() {
		defer healthServer.Shutdown()
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC server stop", "err", err)
		}
	}()
	log.Info("Started gRPC server", "on", addr)
	return grpcServer, nil
}
