package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	datadir2 "github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

func init() {
	withBlock(stateRootCmd)
	withDataDir(stateRootCmd)
	rootCmd.AddCommand(stateRootCmd)
}

var stateRootCmd = &cobra.Command{
	Use:   "stateroot",
	Short: "Exerimental command to re-execute blocks from beginning and compute state root",
	RunE: func(cmd *cobra.Command, args []string) error {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "stateroot"); err != nil {
			logger.Error("Setting up", "error", err)
			return err
		}
		return StateRoot(genesis, logger, block, datadirCli)
	},
}

func blocksIO(db kv.RoDB) (services.FullBlockReader, *blockio.BlockWriter) {
	var histV3, transactionsV3 bool
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		transactionsV3, _ = kvcfg.TransactionsV3.Enabled(tx)
		histV3, _ = kvcfg.HistoryV3.Enabled(tx)
		return nil
	}); err != nil {
		panic(err)
	}
	br := snapshotsync.NewBlockReader(snapshotsync.NewRoSnapshots(ethconfig.Snapshot{Enabled: false}, "", log.New()), transactionsV3)
	bw := blockio.NewBlockWriter(histV3, transactionsV3)
	return br, bw
}

func StateRoot(genesis *types.Genesis, logger log.Logger, blockNum uint64, datadir string) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	dirs := datadir2.New(datadir)
	historyDb, err := kv2.NewMDBX(logger).Path(dirs.Chaindata).Open()
	if err != nil {
		return err
	}
	defer historyDb.Close()
	ctx := context.Background()
	historyTx, err1 := historyDb.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	stateDbPath := filepath.Join(datadir, "staterootdb")
	if _, err = os.Stat(stateDbPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err = os.RemoveAll(stateDbPath); err != nil {
		return err
	}
	db, err2 := kv2.NewMDBX(logger).Path(stateDbPath).Open()
	if err2 != nil {
		return err2
	}
	defer db.Close()
	blockReader, _ := blocksIO(db)

	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	noOpWriter := state.NewNoopWriter()
	interrupt := false
	block := uint64(0)
	var rwTx kv.RwTx
	defer func() {
		rwTx.Rollback()
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	_, genesisIbs, err4 := core.GenesisToBlock(genesis, "")
	if err4 != nil {
		return err4
	}
	w := state.NewPlainStateWriter(rwTx, nil, 0)
	if err = genesisIbs.CommitBlock(&chain2.Rules{}, w); err != nil {
		return fmt.Errorf("cannot write state: %w", err)
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	var tx kv.Tx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	for !interrupt {
		block++
		if block >= blockNum {
			break
		}
		var b *types.Block
		b, err = blockReader.BlockByNumber(ctx, historyTx, block)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		if tx, err = db.BeginRo(ctx); err != nil {
			return err
		}
		if rwTx, err = db.BeginRw(ctx); err != nil {
			return err
		}
		w = state.NewPlainStateWriter(rwTx, nil, block)
		r := state.NewPlainStateReader(tx)
		intraBlockState := state.New(r)
		getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
			h, _ := blockReader.Header(ctx, historyTx, hash, number)
			return h
		}
		if _, err = runBlock(ethash.NewFullFaker(), intraBlockState, noOpWriter, w, chainConfig, getHeader, b, vmConfig, false); err != nil {
			return fmt.Errorf("block %d: %w", block, err)
		}
		if block+1 == blockNum {
			if err = rwTx.ClearBucket(kv.HashedAccounts); err != nil {
				return err
			}
			if err = rwTx.ClearBucket(kv.HashedStorage); err != nil {
				return err
			}
			if err = stagedsync.PromoteHashedStateCleanly("hashedstate", rwTx, stagedsync.StageHashStateCfg(nil, dirs, false), ctx, logger); err != nil {
				return err
			}
			var root libcommon.Hash
			root, err = trie.CalcRoot("genesis", rwTx)
			if err != nil {
				return err
			}
			fmt.Printf("root for block %d=[%x]\n", block, root)
		}
		if block%1000 == 0 {
			logger.Info("Processed", "blocks", block)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
		tx.Rollback()
		if err = rwTx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
