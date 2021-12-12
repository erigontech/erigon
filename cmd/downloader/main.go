package main

import (
	"fmt"
	"net"
	"os"
	"path"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	datadir           string
	seeding           bool
	downloaderApiAddr string
	healthCheck       bool
)

func init() {
	flags := append(debug.Flags, utils.MetricFlags...)
	utils.CobraFlags(rootCmd, flags)

	rootCmd.Flags().StringVar(&datadir, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}

	rootCmd.PersistentFlags().BoolVar(&seeding, "seeding", true, "Seed snapshots")
	rootCmd.Flags().StringVar(&downloaderApiAddr, "downloader.api.addr", "127.0.0.1:9093", "external downloader api network address, for example: 127.0.0.1:9093 serves remote downloader interface")
	rootCmd.Flags().BoolVar(&healthCheck, "healthcheck", false, "Enable grpc health check")
}

func main() {
	ctx, cancel := utils.RootContext()
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
		ctx := cmd.Context()
		snapshotsDir := path.Join(datadir, "snapshots")
		log.Info("Run snapshot downloader", "addr", downloaderApiAddr, "datadir", datadir, "seeding", seeding)

		bittorrentServer, err := downloader.NewServer(snapshotsDir, seeding)
		if err != nil {
			return fmt.Errorf("new server: %w", err)
		}
		log.Info("Load")

		err = bittorrentServer.Load(ctx)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}

		go downloader.MainLoop(ctx, bittorrentServer)
		/*
			go func() {
				for {
					select {
					case <-cmd.Context().Done():
						return
					default:
					}

					snapshots, err := bittorrentServer.Snapshots(ctx, &proto_downloader.SnapshotsRequest{})
					if err != nil {
						log.Error("get snapshots", "err", err)
						time.Sleep(time.Minute)
						continue
					}
					stats := bittorrentServer.Stats(context.Background())
					for _, v := range snapshots.Info {
						log.Info("Snapshot "+v.Path, "%", v.Readiness, "peers", stats[v.Path].ConnectedSeeders)
					}
					time.Sleep(time.Minute)
				}
			}()
		*/

		grpcServer, err := StartGrpc(bittorrentServer, downloaderApiAddr, nil, healthCheck)
		if err != nil {
			return err
		}
		<-cmd.Context().Done()
		grpcServer.GracefulStop()

		return nil
	},
}

func StartGrpc(snServer *downloader.SNDownloaderServer, addr string, creds *credentials.TransportCredentials, healthCheck bool) (*grpc.Server, error) {
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
	var healthServer *health.Server
	if healthCheck {
		healthServer = health.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	}

	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}

	go func() {
		if healthCheck {
			defer healthServer.Shutdown()
		}
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC server stop", "err", err)
		}
	}()
	log.Info("Started gRPC server", "on", addr)
	return grpcServer, nil
}
