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
		needCompare bool
		fullTest    bool
		gethURL     string
		erigonURL   string
		blockFrom   uint64
		blockTo     uint64
		latest      bool
		recordFile  string
		errorFile   string
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

	var bench1Cmd = &cobra.Command{
		Use:   "bench1",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench1(erigonURL, gethURL, needCompare, fullTest, blockFrom, blockTo, recordFile)
		},
	}
	with(bench1Cmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord)
	bench1Cmd.Flags().BoolVar(&fullTest, "fullTest", false, "some text")

	var bench2Cmd = &cobra.Command{
		Use:   "bench2",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench2(erigonURL)
		},
	}
	var bench3Cmd = &cobra.Command{
		Use:   "bench3",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench3(erigonURL, gethURL)
		},
	}
	with(bench3Cmd, withErigonUrl, withGethUrl)

	var bench4Cmd = &cobra.Command{
		Use:   "bench4",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench4(erigonURL)
		},
	}
	with(bench4Cmd, withErigonUrl)

	var bench5Cmd = &cobra.Command{
		Use:   "bench5",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench5(erigonURL)
		},
	}
	with(bench5Cmd, withErigonUrl)
	var bench6Cmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench6(erigonURL)
		},
	}
	with(bench6Cmd, withErigonUrl)

	var bench7Cmd = &cobra.Command{
		Use:   "bench7",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench7(erigonURL, gethURL)
		},
	}
	with(bench7Cmd, withErigonUrl, withGethUrl)

	var benchEthGetLogsCmd = &cobra.Command{
		Use:   "benchEthGetLogs",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchEthGetLogs(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthGetLogsCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var bench9Cmd = &cobra.Command{
		Use:   "bench9",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench9(erigonURL, gethURL, needCompare)
		},
	}
	with(bench9Cmd, withErigonUrl, withGethUrl, withNeedCompare)

	var benchTraceBlockByHashCmd = &cobra.Command{
		Use:   "benchTraceBlockByHash",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchTraceBlockByHash(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceBlockByHashCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceTransactionCmd = &cobra.Command{
		Use:   "benchTraceTransaction",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchTraceTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceCallCmd = &cobra.Command{
		Use:   "benchTraceCall",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchTraceCall(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceCallCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchDebugTraceCallCmd = &cobra.Command{
		Use:   "benchDebugTraceCall",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchDebugTraceCall(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchDebugTraceCallCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceCallManyCmd = &cobra.Command{
		Use:   "benchTraceCallMany",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchTraceCallMany(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceCallManyCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceBlockCmd = &cobra.Command{
		Use:   "benchTraceBlock",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchTraceBlock(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceBlockCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchTraceFilterCmd = &cobra.Command{
		Use:   "benchTraceFilter",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchTraceFilter(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
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
			rpctest.BenchTraceReplayTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceReplayTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	var benchEthBlockByNumberCmd = &cobra.Command{
		Use:   "benchBlockByNumber",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchEthGetBlockByNumber(erigonURL)
		},
	}
	with(benchEthBlockByNumberCmd, withErigonUrl)

	var benchEthGetBalanceCmd = &cobra.Command{
		Use:   "benchEthGetBalance",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.BenchEthGetBalance(erigonURL, gethURL, needCompare, blockFrom, blockTo)
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
		benchEthGetBlockByHash,
		benchEthCallCmd,
		benchEthGetTransactionByHashCmd,
		bench1Cmd,
		bench2Cmd,
		bench3Cmd,
		bench4Cmd,
		bench5Cmd,
		bench6Cmd,
		bench7Cmd,
		benchEthGetLogsCmd,
		bench9Cmd,
		benchTraceTransactionCmd,
		benchTraceCallCmd,
		benchDebugTraceCallCmd,
		benchTraceCallManyCmd,
		benchTraceBlockCmd,
		benchTraceFilterCmd,
		benchTxReceiptCmd,
		compareAccountRange,
		benchTraceReplayTransactionCmd,
		benchEthBlockByNumberCmd,
		benchEthGetBalanceCmd,
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
