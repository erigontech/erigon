package main

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"io"
	"os"
	"os/signal"
	"syscall"
)





func main() {
	var (
		ostream log.Handler
		glogger *log.GlogHandler
	)

	usecolor := (isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
	output := io.Writer(os.Stderr)
	if usecolor {
		output = colorable.NewColorableStderr()
	}
	ostream = log.StreamHandler(output, log.TerminalFormat(usecolor))
	glogger = log.NewGlogHandler(ostream)
	log.Root().SetHandler(glogger)
	glogger.Verbosity(log.LvlInfo)

	var (
		needCompare bool
		fullTest bool
		gethURL string
		tgURL string
		blockNum uint64
		chaindata string

	)


	var bench1Cmd = &cobra.Command{
		Use:   "bench1",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench1(needCompare, fullTest)
		},
	}
	bench1Cmd.Flags().BoolVar(&needCompare, "needCompare", false, "some text")
	bench1Cmd.Flags().BoolVar(&fullTest, "fullTest", false, "some text")


	var bench2Cmd = &cobra.Command{
		Use:   "bench2",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench2()
		},
	}
	var bench3Cmd = &cobra.Command{
		Use:   "bench3",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench3()
		},
	}
	var bench4Cmd = &cobra.Command{
		Use:   "bench4",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench4()
		},
	}
	var bench5Cmd = &cobra.Command{
		Use:   "bench5",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench5()
		},
	}
	var bench6Cmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench6()
		},
	}

	var bench7Cmd = &cobra.Command{
		Use:   "bench7",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench7()
		},
	}

	var bench8Cmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench8()
		},
	}

	var bench9Cmd = &cobra.Command{
		Use:   "bench9",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Bench9(needCompare)
		},
	}
	bench9Cmd.Flags().BoolVar(&needCompare, "needCompare", true, "some text")

	var bench10Cmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			err:=rpctest.Bench10()
			if err!=nil {
				log.Error("bench 10 err", "err", err)
			}
		},
	}


	var proofsCmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.Proofs(chaindata, gethURL, blockNum)
		},
	}
	proofsCmd.Flags().StringVar(&chaindata, "chaindata", "", "")

	var fixStateCmd = &cobra.Command{
		Use:   "bench6",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.FixState(chaindata, gethURL)
		},
	}
	fixStateCmd.Flags().StringVar(&chaindata, "chaindata", "", "")

	tmpDataDir:="/media/b00ris/nvme/accrange"
	tmpDataDirOrig:="/media/b00ris/nvme/accrangeorig"
	needRegenerateGethData:=true
	var compareAccountRange = &cobra.Command{
		Use:   "compareAccountRange",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			rpctest.CompareAccountRange(tgURL, gethURL, tmpDataDir, tmpDataDirOrig, blockNum, needRegenerateGethData)
		},
	}
	compareAccountRange.Flags().BoolVar(&needRegenerateGethData, "regenGethData", false)


	var rootCmd = &cobra.Command{Use: "test"}
	rootCmd.Flags().StringVar(&tgURL, "tgUrl", "http://localhost:8545",  "turbogeth rpcdaemon url")
	rootCmd.Flags().StringVar(&gethURL, "gethUrl", "http://localhost:8546",  "geth rpc url")
	rootCmd.Flags().Uint64Var(&blockNum, "block", 2000000, "Block number")

	rootCmd.AddCommand(
		bench1Cmd,
		bench2Cmd,
		bench3Cmd,
		bench4Cmd,
		bench5Cmd,
		bench6Cmd,
		bench7Cmd,
		bench8Cmd,
		bench9Cmd,
		bench10Cmd,
		proofsCmd,
		fixStateCmd,
		compareAccountRange,
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
