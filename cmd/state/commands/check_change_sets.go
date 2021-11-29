package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var (
	historyfile   string
	nocheck       bool
	writeReceipts bool
)

func init() {
	withBlock(checkChangeSetsCmd)
	withDatadir(checkChangeSetsCmd)
	checkChangeSetsCmd.Flags().StringVar(&historyfile, "historyfile", "", "path to the file where the changesets and history are expected to be. If omitted, the same as <datadir>/erion/chaindata")
	checkChangeSetsCmd.Flags().BoolVar(&nocheck, "nocheck", false, "set to turn off the changeset checking and only execute transaction (for performance testing)")
	checkChangeSetsCmd.Flags().BoolVar(&writeReceipts, "writeReceipts", false, "set to turn on writing receipts as the execution ongoing")
	rootCmd.AddCommand(checkChangeSetsCmd)
}

var checkChangeSetsCmd = &cobra.Command{
	Use:   "checkChangeSets",
	Short: "Re-executes historical transactions in read-only mode and checks that their outputs match the database ChangeSets",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return CheckChangeSets(genesis, logger, block, chaindata, historyfile, nocheck, writeReceipts)
	},
}

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func CheckChangeSets(genesis *core.Genesis, logger log.Logger, blockNum uint64, chaindata string, historyfile string, nocheck bool, writeReceipts bool) error {
	if len(historyfile) == 0 {
		historyfile = chaindata
	}

	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	db, err := kv2.NewMDBX(logger).Path(chaindata).Open()
	if err != nil {
		return err
	}
	chainDb := db
	defer chainDb.Close()
	historyDb := chainDb
	if chaindata != historyfile {
		historyDb = kv2.MustOpen(historyfile)
	}
	historyTx, err1 := historyDb.BeginRo(context.Background())
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	noOpWriter := state.NewNoopWriter()

	interrupt := false
	rwtx, err := chainDb.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer rwtx.Rollback()

	execAt, err1 := stages.GetStageProgress(rwtx, stages.Execution)
	if err1 != nil {
		return err1
	}
	historyAt, err1 := stages.GetStageProgress(rwtx, stages.StorageHistoryIndex)
	if err1 != nil {
		return err1
	}

	commitEvery := time.NewTicker(30 * time.Second)
	defer commitEvery.Stop()
	for !interrupt {

		if blockNum > execAt {
			log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than Exec stage=%d", blockNum, execAt))
			break
		}
		if blockNum > historyAt {
			log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than History stage=%d", blockNum, historyAt))
			break
		}

		blockHash, err := rawdb.ReadCanonicalHash(rwtx, blockNum)
		if err != nil {
			return err
		}
		var block *types.Block
		block, _, err = rawdb.ReadBlockWithSenders(rwtx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			break
		}

		intraBlockState := state.New(state.NewPlainState(historyTx, block.NumberU64()-1))
		csw := state.NewChangeSetWriterPlain(nil /* db */, block.NumberU64()-1)
		var blockWriter state.StateWriter
		if nocheck {
			blockWriter = noOpWriter
		} else {
			blockWriter = csw
		}

		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(rwtx, hash, number) }
		contractHasTEVM := ethdb.GetHasTEVM(rwtx)
		receipts, err1 := runBlock(intraBlockState, noOpWriter, blockWriter, chainConfig, getHeader, contractHasTEVM, block, vmConfig)
		if err1 != nil {
			return err1
		}
		if writeReceipts {
			if chainConfig.IsByzantium(block.NumberU64()) {
				receiptSha := types.DeriveSha(receipts)
				if receiptSha != block.ReceiptHash() {
					return fmt.Errorf("mismatched receipt headers for block %d", block.NumberU64())
				}
			}

			if err := rawdb.AppendReceipts(rwtx, block.NumberU64(), receipts); err != nil {
				return err
			}
		}

		if !nocheck {
			accountChanges, err := csw.GetAccountChanges()
			if err != nil {
				return err
			}
			sort.Sort(accountChanges)
			i := 0
			match := true
			err = changeset.ForPrefix(historyTx, kv.AccountChangeSet, dbutils.EncodeBlockNumber(blockNum), func(blockN uint64, k, v []byte) error {
				c := accountChanges.Changes[i]
				if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
					i++
					return nil
				}

				match = false
				fmt.Printf("Unexpected account changes in block %d\n", blockNum)
				fmt.Printf("In the database: ======================\n")
				fmt.Printf("%d: 0x%x: %x\n", i, k, v)
				fmt.Printf("Expected: ==========================\n")
				fmt.Printf("%d: 0x%x %x\n", i, c.Key, c.Value)
				return nil
			})
			if err != nil {
				return err
			}

			if !match {
				return fmt.Errorf("check change set failed")
			}

			i = 0
			expectedStorageChanges, err := csw.GetStorageChanges()
			if err != nil {
				return err
			}
			if expectedStorageChanges == nil {
				expectedStorageChanges = changeset.NewChangeSet()
			}
			sort.Sort(expectedStorageChanges)
			err = changeset.ForPrefix(historyTx, kv.StorageChangeSet, dbutils.EncodeBlockNumber(blockNum), func(blockN uint64, k, v []byte) error {
				c := expectedStorageChanges.Changes[i]
				i++
				if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
					return nil
				}

				fmt.Printf("Unexpected storage changes in block %d\nIn the database: ======================\n", blockNum)
				fmt.Printf("0x%x: %x\n", k, v)
				fmt.Printf("Expected: ==========================\n")
				fmt.Printf("0x%x %x\n", c.Key, c.Value)
				return fmt.Errorf("check change set failed")
			})
			if err != nil {
				return err
			}
		}

		blockNum++
		if blockNum%1000 == 0 {
			log.Info("Checked", "blocks", blockNum)
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		case <-commitEvery.C:
			if writeReceipts {
				log.Info("Committing receipts", "up to block", block.NumberU64())
				if err = rwtx.Commit(); err != nil {
					return err
				}
				rwtx, err = chainDb.BeginRw(context.Background())
				if err != nil {
					return err
				}
				defer rwtx.Rollback()
				historyTx.Rollback()
				historyTx, err = historyDb.BeginRo(context.Background())
				if err != nil {
					return err
				}
			}
		default:
		}
	}
	if writeReceipts {
		log.Info("Committing final receipts", "batch size")
		if err = rwtx.Commit(); err != nil {
			return err
		}
	}
	log.Info("Checked", "blocks", blockNum, "next time specify --block", blockNum, "duration", time.Since(startTime))
	return nil
}
