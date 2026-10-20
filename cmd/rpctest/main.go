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
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/rpctest/rpctest"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/mem"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/logging"
)

func main() {
	startTime := time.Now()
	var logger log.Logger
	rootCmd := &cobra.Command{
		Use:          "test",
		SilenceUsage: true,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			log.Info(cmd.Name() + " starting")
			logger = debug.SetupCobra(cmd, "rpctest")
			go mem.LogMemStats(cmd.Context(), logger)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			log.Info(cmd.Name(), "took", time.Since(startTime))
			debug.Exit()
		},
	}
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)

	var (
		needCompare       bool
		fullTest          bool
		gethURL           string
		erigonURL         string
		blockFrom         uint64
		blockTo           uint64
		randBlocks        uint64
		concurentRequests uint64
		latest            bool
		recordFile        string
		errorFile         string
		visitAllPages     bool
		additionalParams  string
		failFast          bool
		queryLimit        int
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
	withRandBlockNum := func(cmd *cobra.Command) {
		cmd.Flags().Uint64Var(&randBlocks, "randBlocks", 1000, "Number of random blocks to process")
	}
	withConcurentRequestNum := func(cmd *cobra.Command) {
		cmd.Flags().Uint64Var(&concurentRequests, "concurentRequests", 1, "Number of concurent requests")
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
	withAdditionalParams := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&additionalParams, "additionalParams", "", "Additional params for the request")
	}
	withFailFast := func(cmd *cobra.Command) {
		cmd.Flags().BoolVar(&failFast, "failFast", false, "Fail fast")
	}
	withQueryLimit := func(cmd *cobra.Command) {
		cmd.Flags().IntVar(&queryLimit, "queryLimit", utils.RpcLogQueryLimit.Value,
			"Max addresses or topic alternatives per eth_getLogs request, wider filters are split. Must match the server's --"+utils.RpcLogQueryLimit.Name+" (<=0 = unlimited)")
	}
	with := func(cmd *cobra.Command, opts ...func(*cobra.Command)) {
		for i := range opts {
			opts[i](cmd)
		}
	}

	benchEthCallCmd := &cobra.Command{
		Use:   "benchEthCall",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthCall(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthCallCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	benchEthEstimateGasCmd := &cobra.Command{
		Use:   "benchEthEstimateGas",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthEstimateGas(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthEstimateGasCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchEthCreateAccessListCmd := &cobra.Command{
		Use:   "benchEthCreateAccessList",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthCreateAccessList(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthCreateAccessListCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	benchEthGetBlockByHash := &cobra.Command{
		Use:   "benchEthGetBlockByHash",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetBlockByHash(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthGetBlockByHash, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	benchEthGetBlockByNumber2Cmd := &cobra.Command{
		Use:   "benchEthGetBlockByNumber2",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetBlockByNumber2(erigonURL, gethURL, needCompare, latest, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthGetBlockByNumber2Cmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	benchEthGetTransactionByHashCmd := &cobra.Command{
		Use:   "benchEthGetTransactionByHash",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetTransactionByHash(cmd.Context(), erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthGetTransactionByHashCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	benchOtsGetBlockTransactions := &cobra.Command{
		Use:   "benchOtsGetBlockTransactions",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchOtsGetBlockTransactions(erigonURL, gethURL, needCompare, visitAllPages, latest, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchOtsGetBlockTransactions, withErigonUrl, withGethUrl, withNeedCompare, withVisitAllPages, withBlockNum, withRecord, withErrorFile, withLatest)

	bench1Cmd := &cobra.Command{
		Use:   "bench1",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench1(erigonURL, gethURL, needCompare, fullTest, blockFrom, blockTo, recordFile)
		},
	}
	with(bench1Cmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord)
	bench1Cmd.Flags().BoolVar(&fullTest, "fullTest", false, "some text")

	bench2Cmd := &cobra.Command{
		Use:   "bench2",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench2(erigonURL)
		},
	}
	bench3Cmd := &cobra.Command{
		Use:   "bench3",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench3(erigonURL, gethURL)
		},
	}
	with(bench3Cmd, withErigonUrl, withGethUrl)

	bench4Cmd := &cobra.Command{
		Use:   "bench4",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench4(erigonURL)
		},
	}
	with(bench4Cmd, withErigonUrl)

	bench5Cmd := &cobra.Command{
		Use:   "bench5",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench5(erigonURL)
		},
	}
	with(bench5Cmd, withErigonUrl)
	bench6Cmd := &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench6(erigonURL)
		},
	}
	with(bench6Cmd, withErigonUrl)

	bench7Cmd := &cobra.Command{
		Use:   "bench7",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench7(erigonURL, gethURL)
		},
	}
	with(bench7Cmd, withErigonUrl, withGethUrl)

	benchEthGetLogsCmd := &cobra.Command{
		Use:   "benchEthGetLogs",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetLogs(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchEthGetLogsCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	ethGetLogsInvariantsCmd := &cobra.Command{
		Use:   "invariantsEthGetLogs",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.EthGetLogsInvariants(cmd.Context(), erigonURL, gethURL, needCompare, blockFrom, blockTo, latest, failFast, queryLimit)
		},
	}
	with(ethGetLogsInvariantsCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest, withFailFast, withQueryLimit)

	benchOverlayGetLogsCmd := &cobra.Command{
		Use:   "benchOverlayGetLogs",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchOverlayGetLogs(erigonURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchOverlayGetLogsCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	bench9Cmd := &cobra.Command{
		Use:   "bench9",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.Bench9(erigonURL, gethURL, needCompare, latest)
		},
	}
	with(bench9Cmd, withErigonUrl, withGethUrl, withNeedCompare, withLatest)

	benchTraceCallCmd := &cobra.Command{
		Use:   "benchTraceCall",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTraceCall(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceCallCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	// debug_trace* APIs
	benchDebugTraceBlockByNumberCmd := &cobra.Command{
		Use:   "benchDebugTraceBlockByNumber",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchDebugTraceBlockByNumber(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchDebugTraceBlockByNumberCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile, withLatest)

	benchDebugTraceBlockByHashCmd := &cobra.Command{
		Use:   "benchDebugTraceBlockByHash",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchDebugTraceBlockByHash(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchDebugTraceBlockByHashCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchDebugTraceTransactionCmd := &cobra.Command{
		Use:   "benchDebugTraceTransaction",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchDebugTraceTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, additionalParams, recordFile, errorFile)
		},
	}
	with(benchDebugTraceTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withAdditionalParams, withRecord, withErrorFile)

	benchDebugTraceCallCmd := &cobra.Command{
		Use:   "benchDebugTraceCall",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchDebugTraceCall(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchDebugTraceCallCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	// debug_trace* APIs END

	benchTraceCallManyCmd := &cobra.Command{
		Use:   "benchTraceCallMany",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTraceCallMany(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceCallManyCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchTraceBlockCmd := &cobra.Command{
		Use:   "benchTraceBlock",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTraceBlock(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceBlockCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchTraceFilterCmd := &cobra.Command{
		Use:   "benchTraceFilter",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTraceFilter(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceFilterCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchTxReceiptCmd := &cobra.Command{
		Use:   "benchTxReceipt",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTxReceipt(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTxReceiptCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchBlockReceiptsCmd := &cobra.Command{
		Use:   "benchBlockReceipts",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchBlockReceipts(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchBlockReceiptsCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchTraceReplayTransactionCmd := &cobra.Command{
		Use:   "benchTraceReplayTransaction",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTraceReplayTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceReplayTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchTraceTransactionCmd := &cobra.Command{
		Use:   "benchTraceTransaction",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchTraceTransaction(erigonURL, gethURL, needCompare, blockFrom, blockTo, recordFile, errorFile)
		},
	}
	with(benchTraceTransactionCmd, withGethUrl, withErigonUrl, withNeedCompare, withBlockNum, withRecord, withErrorFile)

	benchEthBlockByNumberCmd := &cobra.Command{
		Use:   "benchBlockByNumber",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetBlockByNumber(erigonURL)
		},
	}
	with(benchEthBlockByNumberCmd, withErigonUrl)

	benchEthGetBalanceCmd := &cobra.Command{
		Use:   "benchEthGetBalance",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetBalance(erigonURL, gethURL, needCompare, blockFrom, blockTo)
		},
	}
	with(benchEthGetBalanceCmd, withErigonUrl, withGethUrl, withNeedCompare, withBlockNum)

	BenchEthGetLogsRandomBlockCmd := &cobra.Command{
		Use:   "benchEthGetLogsRandomBlock",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetLogsRandomBlock(erigonURL, int(concurentRequests))
		},
	}
	with(BenchEthGetLogsRandomBlockCmd, withErigonUrl, withRandBlockNum, withConcurentRequestNum)

	BenchEthGetBalanceRandomAccountCmd := &cobra.Command{
		Use:   "benchEthGetBalanceRandomAccount",
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rpctest.BenchEthGetBalanceRandomAccount(erigonURL, int(concurentRequests))
		},
	}
	with(BenchEthGetBalanceRandomAccountCmd, withErigonUrl, withRandBlockNum, withConcurentRequestNum)

	replayCmd := &cobra.Command{
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
	compareAccountRange := &cobra.Command{
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

	rootCmd.Flags().StringVar(&erigonURL, "erigonUrl", "http://localhost:8545", "Erigon rpcdaemon url")
	rootCmd.Flags().StringVar(&gethURL, "gethUrl", "http://localhost:8546", "geth rpc url")
	rootCmd.Flags().Uint64Var(&blockFrom, "blockFrom", 2000000, "Block number to start test generation from")
	rootCmd.Flags().Uint64Var(&blockTo, "blockTo", 2101000, "Block number to end test generation at")

	rootCmd.AddCommand(
		benchEthGetBlockByNumber2Cmd,
		benchEthGetBlockByHash,
		benchEthCallCmd,
		benchEthEstimateGasCmd,
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
		ethGetLogsInvariantsCmd,
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
		benchBlockReceiptsCmd,
		compareAccountRange,
		benchTraceReplayTransactionCmd,
		benchTraceTransactionCmd,
		benchEthBlockByNumberCmd,
		benchEthGetBalanceCmd,
		BenchEthGetBalanceRandomAccountCmd,
		BenchEthGetLogsRandomBlockCmd,
		benchOtsGetBlockTransactions,
		replayCmd,
	)

	rootCtx, _ := common.RootContext()
	if err := rootCmd.ExecuteContext(rootCtx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
