package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"syscall"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(reconCmd)
	withDataDir(reconCmd)
	rootCmd.AddCommand(reconCmd)
}

var reconCmd = &cobra.Command{
	Use:   "recon",
	Short: "Exerimental command to reconstitute the state from state history at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon(genesis, logger)
	},
}

func Recon(genesis *core.Genesis, logger log.Logger) error {
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
	agg, err := libstate.NewAggregator(aggPath, AggregationStep)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	defer agg.Close()
	reconDbPath := path.Join(datadir, "recondb")
	if block == 0 {
		if _, err = os.Stat(reconDbPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(reconDbPath); err != nil {
			return err
		}
	}
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).WriteMap().Open()
	if err != nil {
		return err
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
	agg.SetTx(rwTx)
	/*
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
	*/
	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.Reopen(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	txNums := make([]uint64, allSnapshots.BlocksAvailable()+1)
	if err = allSnapshots.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
		for _, b := range bs {
			if err = b.Iterate(func(blockNum, baseTxNum, txAmount uint64) {
				txNums[blockNum] = baseTxNum + txAmount
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}
	endTxNumMinimax := agg.EndTxNumMinimax()
	fmt.Printf("Max txNum in files: %d\n", endTxNumMinimax)
	blockNum := uint64(sort.Search(len(txNums), func(i int) bool {
		return txNums[i] > endTxNumMinimax
	}))
	txNum := txNums[blockNum-1]
	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	bitmap := agg.ReconBitmap(txNum)
	fmt.Printf("Bitmap length = %d\n", bitmap.GetCardinality())
	var lastBlockNum uint64
	var lastBlockHash common.Hash
	var lastHeader *types.Header
	var lastSigner *types.Signer
	var lastRules *params.Rules
	stateReader := state.NewHistoryReaderNoState(agg)
	noop := state.NewNoopWriter()
	stateWriter := state.NewStateReconWriter(agg)
	stateReader.SetTx(rwTx)
	stateWriter.SetTx(rwTx)
	it := bitmap.Iterator()
	count := 0
	for it.HasNext() {
		txNum := uint64(it.Next())
		// Find block number
		blockNum := uint64(sort.Search(len(txNums), func(i int) bool {
			return txNums[i] > txNum
		}))
		if blockNum > lastBlockNum {
			if lastHeader, err = blockReader.HeaderByNumber(ctx, nil, blockNum); err != nil {
				return err
			}
			lastBlockNum = blockNum
			lastBlockHash = lastHeader.Hash()
			lastSigner = types.MakeSigner(chainConfig, blockNum)
			lastRules = chainConfig.Rules(blockNum)
		}
		var startTxNum uint64
		if blockNum > 0 {
			startTxNum = txNums[blockNum-1]
		}
		txIndex := txNum - startTxNum - 1
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := blockReader.TxnByIdxInBlock(ctx, nil, blockNum, int(txIndex))
		if err != nil {
			return err
		}
		txHash := txn.Hash()
		msg, err := txn.AsMessage(*lastSigner, lastHeader.BaseFee, lastRules)
		if err != nil {
			return err
		}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		blockCtx, txCtx := transactions.GetEvmContext(msg, lastHeader, true /* requireCanonical */, historyTx, contractHasTEVM, blockReader)
		agg.SetTxNum(txNum)
		stateReader.SetTxNum(txNum)
		stateWriter.SetTxNum(txNum)
		vmConfig := vm.Config{}
		vmConfig.SkipAnalysis = core.SkipAnalysis(chainConfig, blockNum)
		ibs := state.New(stateReader)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
		gp := new(core.GasPool).AddGas(msg.Gas())
		ibs.Prepare(txHash, lastBlockHash, int(txIndex))
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return err
		}
		if err = ibs.FinalizeTx(evm.ChainRules(), noop); err != nil {
			return err
		}
		if err = ibs.CommitBlock(evm.ChainRules(), stateWriter); err != nil {
			return err
		}
		count++
		commit := !it.HasNext()
		if !commit && count%100_000 == 0 {
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
			if rwTx, err = db.BeginRw(ctx); err != nil {
				return err
			}
			agg.SetTx(rwTx)
			stateReader.SetTx(rwTx)
			stateWriter.SetTx(rwTx)
		}
	}
	return nil
}
