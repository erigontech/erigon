package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotdownloader"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotdownloader/bittorrent"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	rootCmd.PersistentFlags().String( "addr", "127.0.0.1:9191", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.PersistentFlags().String( "dir", os.TempDir(), "")
	rootCmd.PersistentFlags().Bool( "seeding", true, "")


}

func Execute() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Config struct {
	Addr string
	Dir string
	Seeding bool
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

func runDownloader(cmd *cobra.Command, args []string) error{

	defer func() {
		log.Info("Stop snapshot downloader")
	}()
	cfg:=&Config{}
	var err error
	cfg.Addr,err = cmd.Flags().GetString("addr")
	if err!=nil {
		return err
	}
	cfg.Dir,err = cmd.Flags().GetString("dir")
	if err!=nil {
		return err
	}
	cfg.Seeding,err = cmd.Flags().GetBool("seeding")
	if err!=nil {
		return err
	}
	log.Info("Run snapshot downloader", "addr", cfg.Addr, "dir", cfg.Dir, "seeding", cfg.Seeding)
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	snapshotdownloader.RegisterDownloaderServer(grpcServer, bittorrent.NewServer(cfg.Dir, cfg.Seeding))
	go func() {
		err:=grpcServer.Serve(lis)
		if err!=nil {
			log.Error("Stop", "err",err)
			cmd.Context().Done()
		}
	}()
	<-cmd.Context().Done()
	return nil

}
