package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	logger := logging.SetupLogger("rpctest")

	var (
		needCompare   bool
		fullTest      bool
		gethURL       string
		erigonURL     string
		blockFrom     uint64
		blockTo       uint64
		latest        bool
		recordFile    string
		errorFile     string
		visitAllPages bool
	)
	withErigonUrl := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&erigonURL, "erigonUrl", "http://localhost:8545", "Erigon rpcdaemon url")
	}
	withGethUrl := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&gethURL, "gethUrl", "http://localhost:8546", "geth rpc url")
	}
	withBlockNum := func(cmd *cobra.Command) {
		cmd.Flags().Uint64Var(&blockFrom, "blockFrom", 2000000, "Block number to start test generation from")
		cmd.Flags().Uint64Var(&blockTo, "blockTo", 2101000, "Block number to end test generation at")
	}
	withLatest := func(cmd *cobra.Command) {
		cmd.Flags().BoolVar(&latest, "latest", false, "Exec on latest ")
	}
	withNeedCompare := func(cmd *cobra.Command) {
		cmd.Flags().BoolVar(&needCompare, "needCompare", false, "need compare with geth")
	}
	withRecord := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&recordFile, "recordFile", "", "File where to record requests and responses to")
	}
	withErrorFile := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&errorFile, "errorFile", "", "File where to record errors (when responses do not match)")
	}
	withVisitAllPages := func(cmd *cobra.Command) {
		cmd.Flags().BoolVar(&visitAllPages, "visitAllPages", false, "Visit all pages")
	}
	with := func(cmd *cobra.Command, opts ...func(*cobra.Command)) {
		for i := range opts {
			opts[i](cmd)
		}
	}

	var benchEthCallCmd = &cobra.Command{
		Use:   "benchEthCall",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthCall(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthCallCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	var benchEthCreateAccessListCmd = &cobra.Command{
		Use:   "benchEthCreateAccessList",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthCreateAccessList(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthCreateAccessListCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	var benchEthGetBlockByHash = &cobra.Command{
		Use:   "benchEthGetBlockByHash",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthGetBlockByHash(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthGetBlockByHash, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	var benchEthGetBlockByNumber2Cmd = &cobra.Command{
		Use:   "benchEthGetBlockByNumber2",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthGetBlockByNumber2(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthGetBlockByNumber2Cmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	var benchEthGetTransactionByHashCmd = &cobra.Command{
		Use:   "benchEthGetTransactionByHash",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthGetTransactionByHash(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthGetTransactionByHashCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	var benchOtsGetBlockTransactions = &cobra.Command{
		Use:   "benchOtsGetBlockTransactions",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchOtsGetBlockTransactions(erigonURL, gethURL, needCompare, visitAllPages, latest, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchOtsGetBlockTransactions, withErigonUrl, withGethUrl, withNeedCompare, withVisitAllPages, withBlockNum, withRecord, withErrorFile, withLatest)

	var bench1Cmd = &cobra.Command{
		Use:   "bench1",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench1(erigonURL, gethURL, needCompare, fullTest, blockFrom, blockTo, recordFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench1Cmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord)
	bench1Cmd.Flags().BoolVar(&fullTest, "fullTest", false, "some text")

	var bench2Cmd = &cobra.Command{
		Use:   "bench2",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench2(erigonURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	var bench3Cmd = &cobra.Command{
		Use:   "bench3",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench3(erigonURL, gethURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench3Cmd, withErigonUrl, withGethUrl)

	var bench4Cmd = &cobra.Command{
		Use:   "bench4",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench4(erigonURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench4Cmd, withErigonUrl)

	var bench5Cmd = &cobra.Command{
		Use:   "bench5",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench5(erigonURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench5Cmd, withErigonUrl)
	var bench6Cmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench6(erigonURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench6Cmd, withErigonUrl)

	var bench7Cmd = &cobra.Command{
		Use:   "bench7",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench7(erigonURL, gethURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench7Cmd, withErigonUrl, withGethUrl)

	var benchEthGetLogsCmd = &cobra.Command{
		Use:   "benchEthGetLogs",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthGetLogs(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthGetLogsCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchOverlayGetLogsCmd = &cobra.Command{
		Use:   "benchOverlayGetLogs",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchOverlayGetLogs(erigonURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchOverlayGetLogsCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var bench9Cmd = &cobra.Command{
		Use:   "bench9",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.Bench9(erigonURL, gethURL, needCompare)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(bench9Cmd, withErigonUrl, withGethUrl, withNeedCompare)

	var benchTraceCallCmd = &cobra.Command{
		Use:   "benchTraceCall",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchTraceCall(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchTraceCallCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	// debug_trace* APIs
	var benchDebugTraceBlockByNumberCmd = &cobra.Command{
		Use:   "benchDebugTraceBlockByNumber",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchDebugTraceBlockByNumber(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchDebugTraceBlockByNumberCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	var benchDebugTraceBlockByHashCmd = &cobra.Command{
		Use:   "benchDebugTraceBlockByHash",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchDebugTraceBlockByHash(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchDebugTraceBlockByHashCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchDebugTraceTransactionCmd = &cobra.Command{
		Use:   "benchDebugTraceTransaction",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchDebugTraceTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchDebugTraceTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchDebugTraceCallCmd = &cobra.Command{
		Use:   "benchDebugTraceCall",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchDebugTraceCall(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchDebugTraceCallCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	// debug_trace* APIs END

	var benchTraceCallManyCmd = &cobra.Command{
		Use:   "benchTraceCallMany",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchTraceCallMany(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchTraceCallManyCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceBlockCmd = &cobra.Command{
		Use:   "benchTraceBlock",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchTraceBlock(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchTraceBlockCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceFilterCmd = &cobra.Command{
		Use:   "benchTraceFilter",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchTraceFilter(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchTraceFilterCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTxReceiptCmd = &cobra.Command{
		Use:   "benchTxReceipt",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchTxReceipt(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchTxReceiptCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceReplayTransactionCmd = &cobra.Command{
		Use:   "benchTraceReplayTransaction",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchTraceReplayTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchTraceReplayTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchEthBlockByNumberCmd = &cobra.Command{
		Use:   "benchBlockByNumber",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthGetBlockByNumber(erigonURL)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthBlockByNumberCmd, withErigonUrl)

	var benchEthGetBalanceCmd = &cobra.Command{
		Use:   "benchEthGetBalance",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err := rpctest.BenchEthGetBalance(erigonURL, gethURL, needCompare, blockFrom, blockTo)
			if err != nil {
				logger.Error(err.Error())
			}
		},
	}
	with(benchEthGetBalanceCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum)

	var replayCmd = &cobra.Command{
		Use:   "replay",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Replay(erigonURL, recordFile)
		},
	}
	with(replayCmd, withErigonUrl, withRecord)

	var tmpDataDir, tmpDataDirOrig string
	var notRegenerateGethData bool
	var compareAccountRange = &cobra.Command{
		Use:   "compareAccountRange",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.CompareAccountRange(log.New(), erigonURL, gethURL, tmpDataDir, tmpDataDirOrig, blockFrom, notRegenerateGethData)
		},
	}
	with(compareAccountRange, withErigonUrl, withGethUrl, withBlockNum)
	compareAccountRange.Flags().BoolVar(&notRegenerateGethData, "regenGethData", false, "")
	compareAccountRange.Flags().StringVar(&tmpDataDir, "tmpdir", "/media/b00ris/nvme/accrange1", "dir for tmp db")
	compareAccountRange.Flags().StringVar(&tmpDataDirOrig, "gethtmpdir", "/media/b00ris/nvme/accrangeorig1", "dir for tmp db")

	var rootCmd = &cobra.Command{Use: "test"}
	rootCmd.Flags().StringVar(&erigonURL, "erigonUrl", "http://localhost:8545", "Erigon rpcdaemon url")
	rootCmd.Flags().StringVar(&gethURL, "gethUrl", "http://localhost:8546", "geth rpc url")
	rootCmd.Flags().Uint64Var(&blockFrom, "blockFrom", 2000000, "Block number to start test generation from")
	rootCmd.Flags().Uint64Var(&blockTo, "blockTo", 2101000, "Block number to end test generation at")

	rootCmd.AddCommand(
		benchEthGetBlockByNumber2Cmd,
		benchEthGetBlockByHash,
		benchEthCallCmd,
		benchEthCreateAccessListCmd,
		benchEthGetTransactionByHashCmd,
		bench1Cmd,
		bench2Cmd,
		bench3Cmd,
		bench4Cmd,
		bench5Cmd,
		bench6Cmd,
		bench7Cmd,
		benchEthGetLogsCmd,
		benchOverlayGetLogsCmd,
		bench9Cmd,
		benchTraceCallCmd,
		benchTraceCallManyCmd,
		benchTraceBlockCmd,
		benchTraceFilterCmd,
		benchDebugTraceBlockByNumberCmd,
		benchDebugTraceBlockByHashCmd,
		benchDebugTraceTransactionCmd,
		benchDebugTraceCallCmd,
		benchTxReceiptCmd,
		compareAccountRange,
		benchTraceReplayTransactionCmd,
		benchEthBlockByNumberCmd,
		benchEthGetBalanceCmd,
		benchOtsGetBlockTransactions,
		replayCmd,
	)
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
