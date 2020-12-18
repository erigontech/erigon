package commands

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"github.com/spf13/cobra"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func init() {
	flags := append(debug.Flags, utils.MetricFlags...)
	flags = append(flags, PreDownloadMainnetFlag, Addr, Dir)
	utils.CobraFlags(rootCmd, flags)

	rootCmd.PersistentFlags().Bool("seeding", true, "Seed snapshots")
}

var (
	Addr = cli.StringFlag{
		Name:  "addr",
		Usage: "external downloader api network address, for example: 127.0.0.1:9191 serves remote downloader interface",
		Value: "127.0.0.1:9191",
	}
	Dir = cli.StringFlag{
		Name:  "dir",
		Usage: "directory to store snapshots",
		Value: os.TempDir(),
	}
	PreDownloadMainnetFlag = cli.BoolFlag{
		Name:  "predownload.mainnet",
		Usage: "add all available mainnet snapshots for seeding",
	}
)

type Config struct {
	Addr    string
	Dir     string
	Seeding bool
}

func Execute() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
}

var rootCmd = &cobra.Command{
	Use:     "",
	Short:   "run snapshot downloader",
	Example: "go run ./cmd/snapshots/downloader/main.go --dir /tmp --addr 127.0.0.1:9191",
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
	cfg := &Config{}
	var err error
	cfg.Addr, err = cmd.Flags().GetString(Addr.Name)
	if err != nil {
		return err
	}
	cfg.Dir, err = cmd.Flags().GetString(Dir.Name)
	if err != nil {
		return err
	}
	cfg.Seeding, err = cmd.Flags().GetBool("seeding")
	if err != nil {
		return err
	}
	log.Info("Run snapshot downloader", "addr", cfg.Addr, "dir", cfg.Dir, "seeding", cfg.Seeding)
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	var opts []grpc.ServerOption
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())

	cpus := uint32(runtime.GOMAXPROCS(-1))
	opts = []grpc.ServerOption{
		grpc.NumStreamWorkers(cpus),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 10 * time.Minute,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer := grpc.NewServer(opts...)
	bittorrentServer, err := bittorrent.NewServer(cfg.Dir, cfg.Seeding)
	if err != nil {
		return err
	}
	err = bittorrentServer.Load()
	if err != nil {
		return err
	}

	mainNetPreDownload, err := cmd.Flags().GetBool(PreDownloadMainnetFlag.Name)
	if err != nil {
		return err
	}
	if mainNetPreDownload {
		log.Info("Predownload mainnet snapshots")
		go func() {
			_, err := bittorrentServer.Download(context.Background(), &snapshotsync.DownloadSnapshotRequest{
				NetworkId: params.MainnetChainConfig.ChainID.Uint64(),
				Type:      bittorrent.GetAvailableSnapshotTypes(params.MainnetChainConfig.ChainID.Uint64()),
			})
			if err != nil {
				log.Error("Predownload failed", "err", err, "networkID", params.MainnetChainConfig.ChainID.Uint64())
			}
		}()
	}
	snapshotsync.RegisterDownloaderServer(grpcServer, bittorrentServer)
	go func() {
		log.Info("Starting grpc")
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Error("Stop", "err", err)
		}
	}()
	<-cmd.Context().Done()
	return nil

}
