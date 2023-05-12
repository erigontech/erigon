package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/txpool/txpooluitl"
	"github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	common2 "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
)

var (
	sentryAddr     []string // Address of the sentry <host>:<port>
	traceSenders   []string
	privateApiAddr string
	txpoolApiAddr  string
	datadirCli     string // Path to td working dir

	TLSCertfile string
	TLSCACert   string
	TLSKeyFile  string

	pendingPoolLimit int
	baseFeePoolLimit int
	queuedPoolLimit  int

	priceLimit   uint64
	accountSlots uint64
	priceBump    uint64

	commitEvery time.Duration
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	rootCmd.Flags().StringSliceVar(&sentryAddr, "sentry.api.addr", []string{"localhost:9091"}, "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	rootCmd.Flags().StringVar(&privateApiAddr, "private.api.addr", "localhost:9090", "execution service <host>:<port>")
	rootCmd.Flags().StringVar(&txpoolApiAddr, "txpool.api.addr", "localhost:9094", "txpool service <host>:<port>")
	rootCmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}
	rootCmd.PersistentFlags().StringVar(&TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&TLSKeyFile, "tls.key", "", "key file for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake")

	rootCmd.PersistentFlags().IntVar(&pendingPoolLimit, "txpool.globalslots", txpoolcfg.DefaultConfig.PendingSubPoolLimit, "Maximum number of executable transaction slots for all accounts")
	rootCmd.PersistentFlags().IntVar(&baseFeePoolLimit, "txpool.globalbasefeeslots", txpoolcfg.DefaultConfig.BaseFeeSubPoolLimit, "Maximum number of non-executable transactions where only not enough baseFee")
	rootCmd.PersistentFlags().IntVar(&queuedPoolLimit, "txpool.globalqueue", txpoolcfg.DefaultConfig.QueuedSubPoolLimit, "Maximum number of non-executable transaction slots for all accounts")
	rootCmd.PersistentFlags().Uint64Var(&priceLimit, "txpool.pricelimit", txpoolcfg.DefaultConfig.MinFeeCap, "Minimum gas price (fee cap) limit to enforce for acceptance into the pool")
	rootCmd.PersistentFlags().Uint64Var(&accountSlots, "txpool.accountslots", txpoolcfg.DefaultConfig.AccountSlots, "Minimum number of executable transaction slots guaranteed per account")
	rootCmd.PersistentFlags().Uint64Var(&priceBump, "txpool.pricebump", txpoolcfg.DefaultConfig.PriceBump, "Price bump percentage to replace an already existing transaction")
	rootCmd.PersistentFlags().DurationVar(&commitEvery, utils.TxPoolCommitEveryFlag.Name, utils.TxPoolCommitEveryFlag.Value, utils.TxPoolCommitEveryFlag.Usage)
	rootCmd.Flags().StringSliceVar(&traceSenders, utils.TxPoolTraceSendersFlag.Name, []string{}, utils.TxPoolTraceSendersFlag.Usage)
}

var rootCmd = &cobra.Command{
	Use:   "txpool",
	Short: "Launch external Transaction Pool instance - same as built-into Erigon, but as independent Process",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	Run: func(cmd *cobra.Command, args []string) {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "txpool"); err != nil {
			logger.Error("Setting up", "error", err)
			return
		}

		if err := doTxpool(cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

func doTxpool(ctx context.Context, logger log.Logger) error {
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

	log.Info("TxPool started", "db", filepath.Join(datadirCli, "txpool"))

	sentryClients := make([]direct.SentryClient, len(sentryAddr))
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
	}

	cfg := txpoolcfg.DefaultConfig
	dirs := datadir.New(datadirCli)

	cfg.DBDir = dirs.TxPool

	cfg.CommitEvery = common2.RandomizeDuration(commitEvery)
	cfg.PendingSubPoolLimit = pendingPoolLimit
	cfg.BaseFeeSubPoolLimit = baseFeePoolLimit
	cfg.QueuedSubPoolLimit = queuedPoolLimit
	cfg.MinFeeCap = priceLimit
	cfg.AccountSlots = accountSlots
	cfg.PriceBump = priceBump

	cacheConfig := kvcache.DefaultCoherentConfig
	cacheConfig.MetricsLabel = "txpool"

	cfg.TracedSenders = make([]string, len(traceSenders))
	for i, senderHex := range traceSenders {
		sender := common.HexToAddress(senderHex)
		cfg.TracedSenders[i] = string(sender[:])
	}

	newTxs := make(chan types.Announcements, 1024)
	defer close(newTxs)
	txPoolDB, txPool, fetch, send, txpoolGrpcServer, err := txpooluitl.AllComponents(ctx, cfg,
		kvcache.New(cacheConfig), newTxs, coreDB, sentryClients, kvClient, logger)
	if err != nil {
		return err
	}
	fetch.ConnectCore()
	fetch.ConnectSentries()

	/*
		var ethashApi *ethash.API
		sif casted, ok := backend.engine.(*ethash.Ethash); ok {
			ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
		}
	*/
	miningGrpcServer := privateapi.NewMiningServer(ctx, &rpcdaemontest.IsMiningMock{}, nil)

	grpcServer, err := txpool.StartGrpc(txpoolGrpcServer, miningGrpcServer, txpoolApiAddr, nil, logger)
	if err != nil {
		return err
	}

	notifyMiner := func() {}
	txpool.MainLoop(ctx, txPoolDB, coreDB, txPool, newTxs, send, txpoolGrpcServer.NewSlotsStreams, notifyMiner)

	grpcServer.GracefulStop()
	return nil
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
