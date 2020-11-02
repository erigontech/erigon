package commands

import (
	"context"
	"fmt"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	rootCmd.PersistentFlags().String("addr", "127.0.0.1:9191", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.PersistentFlags().String("dir", os.TempDir(), "")
	rootCmd.PersistentFlags().Bool("seeding", true, "")

}

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
	Use:   "",
	Short: "run snapshot downloader",
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

	defer func() {
		log.Info("Stop snapshot downloader")
	}()
	cfg := &Config{}
	var err error
	cfg.Addr, err = cmd.Flags().GetString("addr")
	if err != nil {
		return err
	}
	cfg.Dir, err = cmd.Flags().GetString("dir")
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
		grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024),  // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(100), // to force clients reduce concurrency level
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 10 * time.Minute,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer := grpc.NewServer(opts...)
	snapshotsync.RegisterDownloaderServer(grpcServer, bittorrent.NewServer(cfg.Dir, cfg.Seeding))
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Error("Stop", "err", err)
			cmd.Context().Done()
		}
	}()
	<-cmd.Context().Done()
	return nil

}
