package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(history22Cmd)
	withDataDir(history22Cmd)
	history22Cmd.Flags().IntVar(&traceBlock, "traceblock", 0, "block number at which to turn on tracing")
	history22Cmd.Flags().IntVar(&blockTo, "blockto", 0, "block number to stop replay of history at")
	rootCmd.AddCommand(history22Cmd)
}

var history22Cmd = &cobra.Command{
	Use:   "history22",
	Short: "Exerimental command to re-execute historical transactions in erigon2 format",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return History22(genesis, logger)
	},
}

func History22(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	historyDb, err := kv2.NewMDBX(logger).Path(path.Join(datadir, "chaindata")).Open()
	if err != nil {
		return fmt.Errorf("opening chaindata as read only: %v", err)
	}
	defer historyDb.Close()
	ctx := context.Background()
	historyTx, err1 := historyDb.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	aggPath := filepath.Join(datadir, "erigon22")
	h, err3 := libstate.NewAggregator(aggPath, AggregationStep)
	//h, err3 := aggregator.NewHistory(aggPath, uint64(blockTo), aggregationStep)
	if err3 != nil {
		return fmt.Errorf("create history: %w", err3)
	}
	defer h.Close()
	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	interrupt := false
	blockNum := uint64(0)
	var txNum uint64 = 2
	trace := false
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	prevBlock := blockNum
	prevTime := time.Now()

	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.Reopen(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)

	for !interrupt {
		select {
		default:
		case <-logEvery.C:
			currentTime := time.Now()
			interval := currentTime.Sub(prevTime)
			speed := float64(blockNum-prevBlock) / (float64(interval) / float64(time.Second))
			prevBlock = blockNum
			prevTime = currentTime
			log.Info("Progress", "block", blockNum, "blk/s", speed)
		}
		blockNum++
		if blockNum > uint64(blockTo) {
			break
		}
		blockHash, err := rawdb.ReadCanonicalHash(historyTx, blockNum)
		if err != nil {
			return err
		}
		b, _, err := blockReader.BlockWithSenders(ctx, historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			log.Info("history: block is nil", "block", blockNum)
			break
		}
		if blockNum <= block {
			// Skip that block, but increase txNum
			txNum += uint64(len(b.Transactions())) + 2 // Pre and Post block transaction
			continue
		}
		readWrapper := state.NewHistoryReader22(h)
		if traceBlock != 0 {
			readWrapper.SetTrace(blockNum == uint64(traceBlock))
		}
		writeWrapper := state.NewNoopWriter()
		txNum++ // Pre block transaction
		getHeader := func(hash common.Hash, number uint64) *types.Header {
			h, err := blockReader.Header(ctx, historyTx, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		}
		if txNum, _, err = runHistory22(trace, blockNum, txNum, readWrapper, writeWrapper, chainConfig, getHeader, b, vmConfig); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		txNum++ // Post block transaction
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --block %d", blockNum))
		default:
		}
	}
	return nil
}

func runHistory22(trace bool, blockNum, txNumStart uint64, hw *state.HistoryReader22, ww state.StateWriter, chainConfig *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	daoBlock := chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	txNum := txNumStart
	for i, tx := range block.Transactions() {
		hw.SetTxNum(txNum)
		ibs := state.New(hw)
		if daoBlock {
			misc.ApplyDAOHardFork(ibs)
			daoBlock = false
		}
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig, nil)
		if err != nil {
			return 0, nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		if traceBlock != 0 && blockNum == uint64(traceBlock) {
			fmt.Printf("tx idx %d, num %d, gas used %d\n", i, txNum, receipt.GasUsed)
		}
		receipts = append(receipts, receipt)
		txNum++
		hw.SetTxNum(txNum)
	}

	return txNum, receipts, nil
}
