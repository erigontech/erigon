package main

import (
	"fmt"
	"os"
	"path"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/ethdb/remotedbserver"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var (
	sentryAddr     []string // Address of the sentry <host>:<port>
	privateApiAddr string
	txpoolApiAddr  string
	datadir        string // Path to td working dir

	TLSCertfile string
	TLSCACert   string
	TLSKeyFile  string
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	rootCmd.Flags().StringSliceVar(&sentryAddr, "sentry.api.addr", []string{"localhost:9091"}, "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	rootCmd.Flags().StringVar(&privateApiAddr, "private.api.addr", "localhost:9090", "execution service <host>:<port>")
	rootCmd.Flags().StringVar(&txpoolApiAddr, "txpool.api.addr", "localhost:9094", "txpool service <host>:<port>")
	rootCmd.Flags().StringVar(&datadir, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}
	rootCmd.PersistentFlags().StringVar(&TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&TLSKeyFile, "tls.key", "", "key file for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake")

}

var rootCmd = &cobra.Command{
	Use:   "sentry",
	Short: "Run p2p sentry",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return debug.SetupCobra(cmd)
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		creds, err := grpcutil.TLS(TLSCACert, TLSCertfile, TLSKeyFile)
		if err != nil {
			return fmt.Errorf("could not connect to remoteKv: %w", err)
		}
		coreConn, err := grpcutil.Connect(creds, privateApiAddr)
		if err != nil {
			return fmt.Errorf("could not connect to remoteKv: %w", err)
		}

		kvClient := remote.NewKVClient(coreConn)
		coreDB, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), log.New(), kvClient).Open()
		if err != nil {
			return fmt.Errorf("could not connect to remoteKv: %w", err)
		}

		log.Info("TxPool started", "db", path.Join(datadir, "txpool"))
		txPoolDB, err := mdbx.NewMDBX(log.New()).Path(path.Join(datadir, "txpool")).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).Open()
		if err != nil {
			return err
		}

		sentryClients := make([]txpool.SentryClient, len(sentryAddr))
		sentryClientsCasted := make([]proto_sentry.SentryClient, len(sentryAddr))
		for i := range sentryAddr {
			creds, err := grpcutil.TLS(TLSCACert, TLSCertfile, TLSKeyFile)
			if err != nil {
				return fmt.Errorf("could not connect to sentry: %w", err)
			}
			sentryConn, err := grpcutil.Connect(creds, sentryAddr[i])
			if err != nil {
				return fmt.Errorf("could not connect to sentry: %w", err)
			}

			sentryClients[i] = direct.NewSentryClientRemote(proto_sentry.NewSentryClient(sentryConn))
			sentryClientsCasted[i] = proto_sentry.SentryClient(sentryClients[i])
		}

		newTxs := make(chan txpool.Hashes, 1024)
		txPool, err := txpool.New(newTxs, txPoolDB, coreDB, txpool.DefaultConfig)
		if err != nil {
			return err
		}

		fetcher := txpool.NewFetch(cmd.Context(), sentryClientsCasted, txPool, kvClient, coreDB, txPoolDB)
		fetcher.ConnectCore()
		fetcher.ConnectSentries()

		send := txpool.NewSend(cmd.Context(), sentryClients, txPool)
		txpoolGrpcServer := txpool.NewGrpcServer(cmd.Context(), txPool, txPoolDB)
		/*
			var ethashApi *ethash.API
			if casted, ok := backend.engine.(*ethash.Ethash); ok {
				ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
			}
			miningGrpcServer := privateapi.NewMiningServer(cmd.Context(), &rpcdaemontest.IsMiningMock{}, ethashApi)
		*/

		grpcServer, err := txpool.StartGrpc(txpoolGrpcServer, nil, txpoolApiAddr, nil)
		if err != nil {
			return err
		}

		txpool.MainLoop(cmd.Context(), txPoolDB, coreDB, txPool, newTxs, send, txpoolGrpcServer.NewSlotsStreams)

		grpcServer.GracefulStop()
		return nil
	},
}

func main() {
	ctx, cancel := utils.RootContext()
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
