package commands

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"sort"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var txhash string
var txnum uint64

func init() {
	withDataDir(replayTxCmd)
	rootCmd.AddCommand(replayTxCmd)
	replayTxCmd.Flags().StringVar(&txhash, "txhash", "", "hash of the transaction to replay")
	replayTxCmd.Flags().Uint64Var(&txnum, "txnum", 0, "tx num for replay")
}

var replayTxCmd = &cobra.Command{
	Use:   "replaytx",
	Short: "Experimental command to replay a given transaction using only history",
	RunE: func(cmd *cobra.Command, args []string) error {
		return ReplayTx(genesis)
	},
}

func ReplayTx(genesis *core.Genesis) error {
	var blockReader services.FullBlockReader
	var allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, true, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	txNums := make([]uint64, allSnapshots.BlocksAvailable()+1)
	if err := allSnapshots.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
		for _, b := range bs {
			if err := b.Iterate(func(blockNum, baseTxNum, txAmount uint64) {
				txNums[blockNum] = baseTxNum + txAmount
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}
	ctx := context.Background()
	var txNum uint64
	if txhash != "" {
		txnHash := common.HexToHash(txhash)
		fmt.Printf("Tx hash = [%x]\n", txnHash)
		db := memdb.New()
		roTx, err := db.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer roTx.Rollback()
		bn, ok, err := blockReader.TxnLookup(ctx, roTx, txnHash)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("transaction not found")
		}
		fmt.Printf("Found in block %d\n", bn)
		var header *types.Header
		if header, err = blockReader.HeaderByNumber(ctx, nil, bn); err != nil {
			return err
		}
		blockHash := header.Hash()
		b, _, err := blockReader.BlockWithSenders(ctx, nil, blockHash, bn)
		if err != nil {
			return err
		}
		txs := b.Transactions()
		var txIndex int
		for txIndex = 0; txIndex < len(txs); txIndex++ {
			if txs[txIndex].Hash() == txnHash {
				fmt.Printf("txIndex = %d\n", txIndex)
				break
			}
		}
		txNum = txNums[bn-1] + 1 + uint64(txIndex)
	} else {
		txNum = txnum
	}
	fmt.Printf("txNum = %d\n", txNum)
	aggPath := filepath.Join(datadir, "agg22")
	agg, err := libstate.NewAggregator22(aggPath, AggregationStep)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	defer agg.Close()
	ac := agg.MakeContext()
	workCh := make(chan *state.TxTask)
	rs := state.NewReconState(workCh)
	if err = replayTxNum(ctx, allSnapshots, blockReader, txNum, txNums, rs, ac); err != nil {
		return err
	}
	return nil
}

func replayTxNum(ctx context.Context, allSnapshots *snapshotsync.RoSnapshots, blockReader services.FullBlockReader,
	txNum uint64, txNums []uint64, rs *state.ReconState, ac *libstate.Aggregator22Context,
) error {
	bn := uint64(sort.Search(len(txNums), func(i int) bool {
		return txNums[i] > txNum
	}))
	txIndex := int(txNum - txNums[bn-1] - 1)
	fmt.Printf("bn=%d, txIndex=%d\n", bn, txIndex)
	var header *types.Header
	var err error
	if header, err = blockReader.HeaderByNumber(ctx, nil, bn); err != nil {
		return err
	}
	blockHash := header.Hash()
	b, _, err := blockReader.BlockWithSenders(ctx, nil, blockHash, bn)
	if err != nil {
		return err
	}
	txn := b.Transactions()[txIndex]
	stateWriter := state.NewStateReconWriter(ac, rs)
	stateReader := state.NewHistoryReaderNoState(ac, rs)
	stateReader.SetTxNum(txNum)
	stateWriter.SetTxNum(txNum)
	noop := state.NewNoopWriter()
	rules := chainConfig.Rules(bn)
	for {
		stateReader.ResetError()
		ibs := state.New(stateReader)
		gp := new(core.GasPool).AddGas(txn.GetGas())
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, gas=%d, input=[%x]\n", txNum, blockNum, txIndex, txn.GetGas(), txn.GetData())
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(chainConfig, bn)}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		getHeader := func(hash common.Hash, number uint64) *types.Header {
			h, err := blockReader.Header(ctx, nil, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		}
		getHashFn := core.GetHashFn(header, getHeader)
		logger := log.New()
		engine := initConsensusEngine(chainConfig, logger, allSnapshots)
		txnHash := txn.Hash()
		blockContext := core.NewEVMBlockContext(header, getHashFn, engine, nil /* author */, contractHasTEVM)
		ibs.Prepare(txnHash, blockHash, txIndex)
		msg, err := txn.AsMessage(*types.MakeSigner(chainConfig, bn), header.BaseFee, rules)
		if err != nil {
			return err
		}
		txContext := core.NewEVMTxContext(msg)
		vmenv := vm.NewEVM(blockContext, txContext, ibs, chainConfig, vmConfig)

		_, err = core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return fmt.Errorf("could not apply tx %d [%x] failed: %w", txIndex, txnHash, err)
		}
		if err = ibs.FinalizeTx(rules, noop); err != nil {
			return err
		}
		if dependency, ok := stateReader.ReadError(); ok {
			fmt.Printf("dependency %d on %d\n", txNum, dependency)
			if err = replayTxNum(ctx, allSnapshots, blockReader, dependency, txNums, rs, ac); err != nil {
				return err
			}
		} else {
			if err = ibs.CommitBlock(rules, stateWriter); err != nil {
				return err
			}
			break
		}
	}
	rs.CommitTxNum(txNum)
	fmt.Printf("commited %d\n", txNum)
	return nil
}
