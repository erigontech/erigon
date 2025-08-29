// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/remotedb"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/paths"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"
	"github.com/erigontech/erigon/turbo/privateapi"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
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

	priceLimit         uint64
	accountSlots       uint64
	blobSlots          uint64
	totalBlobPoolLimit uint64
	priceBump          uint64
	blobPriceBump      uint64

	noTxGossip bool

	mdbxWriteMap bool

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
	rootCmd.PersistentFlags().Uint64Var(&blobSlots, "txpool.blobslots", txpoolcfg.DefaultConfig.BlobSlots, "Max allowed total number of blobs (within type-3 txs) per account")
	rootCmd.PersistentFlags().Uint64Var(&totalBlobPoolLimit, "txpool.totalblobpoollimit", txpoolcfg.DefaultConfig.TotalBlobPoolLimit, "Total limit of number of all blobs in txs within the txpool")
	rootCmd.PersistentFlags().Uint64Var(&priceBump, "txpool.pricebump", txpoolcfg.DefaultConfig.PriceBump, "Price bump percentage to replace an already existing transaction")
	rootCmd.PersistentFlags().Uint64Var(&blobPriceBump, "txpool.blobpricebump", txpoolcfg.DefaultConfig.BlobPriceBump, "Price bump percentage to replace an existing blob (type-3) transaction")
	rootCmd.PersistentFlags().DurationVar(&commitEvery, utils.TxPoolCommitEveryFlag.Name, utils.TxPoolCommitEveryFlag.Value, utils.TxPoolCommitEveryFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&noTxGossip, utils.TxPoolGossipDisableFlag.Name, utils.TxPoolGossipDisableFlag.Value, utils.TxPoolGossipDisableFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&mdbxWriteMap, utils.DbWriteMapFlag.Name, utils.DbWriteMapFlag.Value, utils.DbWriteMapFlag.Usage)
	rootCmd.Flags().StringSliceVar(&traceSenders, utils.TxPoolTraceSendersFlag.Name, []string{}, utils.TxPoolTraceSendersFlag.Usage)
}

var rootCmd = &cobra.Command{
	Use:   "txpool",
	Short: "Launch external Transaction Pool instance - same as built-into Erigon, but as independent Process",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
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

	ethBackendClient := remote.NewETHBACKENDClient(coreConn)
	kvClient := remote.NewKVClient(coreConn)
	coreDB, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), log.New(), kvClient).Open()
	if err != nil {
		return fmt.Errorf("could not connect to remoteKv: %w", err)
	}

	log.Info("TxPool started", "db", filepath.Join(datadirCli, "txpool"))

	sentryClients := make([]proto_sentry.SentryClient, len(sentryAddr))
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

	cfg.CommitEvery = common.RandomizeDuration(commitEvery)
	cfg.PendingSubPoolLimit = pendingPoolLimit
	cfg.BaseFeeSubPoolLimit = baseFeePoolLimit
	cfg.QueuedSubPoolLimit = queuedPoolLimit
	cfg.MinFeeCap = priceLimit
	cfg.AccountSlots = accountSlots
	cfg.BlobSlots = blobSlots
	cfg.TotalBlobPoolLimit = totalBlobPoolLimit
	cfg.PriceBump = priceBump
	cfg.BlobPriceBump = blobPriceBump
	cfg.NoGossip = noTxGossip
	cfg.MdbxWriteMap = mdbxWriteMap

	cacheConfig := kvcache.DefaultCoherentConfig
	cacheConfig.MetricsLabel = "txpool"

	cfg.TracedSenders = make([]string, len(traceSenders))
	for i, senderHex := range traceSenders {
		sender := common.HexToAddress(senderHex)
		cfg.TracedSenders[i] = string(sender[:])
	}

	notifyMiner := func() {}
	txPool, txpoolGrpcServer, err := txpool.Assemble(
		ctx,
		cfg,
		coreDB,
		kvcache.New(cacheConfig),
		sentryClients,
		kvClient,
		notifyMiner,
		logger,
		ethBackendClient,
	)
	if err != nil {
		return err
	}

	miningGrpcServer := privateapi.NewMiningServer(ctx, &rpcdaemontest.IsMiningMock{}, nil, logger)
	grpcServer, err := txpool.StartGrpc(txpoolGrpcServer, miningGrpcServer, txpoolApiAddr, nil, logger)
	if err != nil {
		return err
	}

	err = txPool.Run(ctx)
	if err != nil {
		return err
	}

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
