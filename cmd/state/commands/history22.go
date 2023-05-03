package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/commitment"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

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
)

var (
	blockTo    int
	traceBlock int
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

func History22(genesis *types.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	dirs := datadir.New(datadirCli)
	historyDb, err := kv2.NewMDBX(logger).Path(dirs.Chaindata).Open()
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
	aggPath := filepath.Join(datadirCli, "erigon23")
	h, err := libstate.NewAggregator(aggPath, dirs.Tmp, ethconfig.HistoryV3AggregationStep, libstate.CommitmentModeDirect, commitment.VariantHexPatriciaTrie)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	if err := h.ReopenFolder(); err != nil {
		return err
	}

	defer h.Close()
	readDbPath := path.Join(datadirCli, "readdb")
	if block == 0 {
		if _, err = os.Stat(readDbPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(readDbPath); err != nil {
			return err
		}
	}
	db, err := kv2.NewMDBX(logger).Path(readDbPath).WriteMap().Open()
	if err != nil {
		return err
	}
	defer db.Close()
	readPath := filepath.Join(datadirCli, "reads")
	if block == 0 {
		if _, err = os.Stat(readPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(readPath); err != nil {
			return err
		}
		if err = os.Mkdir(readPath, os.ModePerm); err != nil {
			return err
		}
	}
	ri, err := libstate.NewReadIndices(readPath, dirs.Tmp, ethconfig.HistoryV3AggregationStep)
	if err != nil {
		return fmt.Errorf("create read indices: %w", err)
	}
	var rwTx kv.RwTx
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	ri.SetTx(rwTx)
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
	allSnapshots := snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadirCli, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenWithDB(db); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots, ethconfig.Defaults.TransactionsV3)
	readWrapper := state.NewHistoryReaderV4(h.MakeContext(), ri)

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
		if traceBlock != 0 {
			readWrapper.SetTrace(blockNum == uint64(traceBlock))
		}
		writeWrapper := state.NewNoopWriter()
		getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
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
		// Commit transaction only when interrupted or just before computing commitment (so it can be re-done)
		commit := interrupt
		if !commit && (blockNum+1)%uint64(commitmentFrequency) == 0 {
			var spaceDirty uint64
			if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
				return fmt.Errorf("retrieving spaceDirty: %w", err)
			}
			if spaceDirty >= dirtySpaceThreshold {
				log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
				commit = true
			}
		}
		if commit {
			if err = rwTx.Commit(); err != nil {
				return err
			}
			if !interrupt {
				if rwTx, err = db.BeginRw(ctx); err != nil {
					return err
				}
			}
			ri.SetTx(rwTx)
		}
	}
	return nil
}

func runHistory22(trace bool, blockNum, txNumStart uint64, hw *state.HistoryReaderV4, ww state.StateWriter, chainConfig *chain2.Config, getHeader func(hash libcommon.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	header := block.Header()
	excessDataGas := header.ParentExcessDataGas(getHeader)
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddDataGas(params.MaxDataGasPerBlock)
	usedGas := new(uint64)
	var receipts types.Receipts
	rules := chainConfig.Rules(block.NumberU64(), block.Time())
	txNum := txNumStart
	hw.SetTxNum(txNum)
	daoFork := chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	if daoFork {
		ibs := state.New(hw)
		misc.ApplyDAOHardFork(ibs)
		if err := ibs.FinalizeTx(rules, ww); err != nil {
			return 0, nil, err
		}
		if err := hw.FinishTx(); err != nil {
			return 0, nil, fmt.Errorf("finish dao fork failed: %w", err)
		}
	}
	txNum++ // Pre block transaction
	for i, tx := range block.Transactions() {
		hw.SetTxNum(txNum)
		ibs := state.New(hw)
		ibs.SetTxContext(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig, excessDataGas)
		if err != nil {
			return 0, nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		if traceBlock != 0 && blockNum == uint64(traceBlock) {
			fmt.Printf("tx idx %d, num %d, gas used %d\n", i, txNum, receipt.GasUsed)
		}
		receipts = append(receipts, receipt)
		if err = hw.FinishTx(); err != nil {
			return 0, nil, fmt.Errorf("finish tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		txNum++
		hw.SetTxNum(txNum)
	}

	return txNum, receipts, nil
}
