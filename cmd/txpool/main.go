package main

import (
	"fmt"
	"os"
	"path"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
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
	datadir        string // Path to td working dir

	TLSCertfile string
	TLSCACert   string
	TLSKeyFile  string
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
	rootCmd.Flags().StringSliceVar(&sentryAddr, "sentry.api.addr", []string{"localhost:9091"}, "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	rootCmd.Flags().StringVar(&privateApiAddr, "private.api.addr", "localhost:9090", "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
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
		coreConn, err := cli.ConnectCore(TLSCertfile, TLSKeyFile, TLSCACert, privateApiAddr)
		if err != nil {
			return fmt.Errorf("could not connect to remoteKv: %w", err)
		}

		kvClient := remote.NewKVClient(coreConn)
		coreDB, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), log.New(), kvClient).Open()
		if err != nil {
			return fmt.Errorf("could not connect to remoteKv: %w", err)
		}

		txPoolDB, err := mdbx.NewMDBX(log.New()).Path(path.Join(datadir, "txpool")).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).Open()
		if err != nil {
			return err
		}

		sentryClients := make([]txpool.SentryClient, len(sentryAddr))
		sentryClientsCasted := make([]proto_sentry.SentryClient, len(sentryAddr))
		for i := range sentryAddr {
			sentryConn, err := cli.ConnectCore(TLSCertfile, TLSKeyFile, TLSCACert, sentryAddr[i])
			if err != nil {
				return fmt.Errorf("could not connect to sentry: %w", err)
			}

			sentryClients[i] = direct.NewSentryClientRemote(proto_sentry.NewSentryClient(sentryConn))
			sentryClientsCasted[i] = proto_sentry.SentryClient(sentryClients[i])
		}

		newTxs := make(chan txpool.Hashes, 1)
		txPool, err := txpool.New(newTxs, txPoolDB)
		if err != nil {
			return err
		}
		senders := txpool.NewSendersCache()

		fetcher := txpool.NewFetch(cmd.Context(), sentryClientsCasted, txPool, senders, kvClient, coreDB)
		fetcher.ConnectCore()
		fetcher.ConnectSentries()

		send := txpool.NewSend(cmd.Context(), sentryClients, txPool)

		txpool.BroadcastLoop(cmd.Context(), txPoolDB, txPool, senders, newTxs, send, txpool.DefaultTimings)
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
