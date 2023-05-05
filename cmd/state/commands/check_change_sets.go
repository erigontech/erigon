package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"sort"
	"syscall"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

var (
	historyfile    string
	nocheck        bool
	transactionsV3 bool
)

func init() {
	withBlock(checkChangeSetsCmd)
	withDataDir(checkChangeSetsCmd)
	withSnapshotBlocks(checkChangeSetsCmd)
	checkChangeSetsCmd.Flags().StringVar(&historyfile, "historyfile", "", "path to the file where the changesets and history are expected to be. If omitted, the same as <datadir>/erion/chaindata")
	checkChangeSetsCmd.Flags().BoolVar(&nocheck, "nocheck", false, "set to turn off the changeset checking and only execute transaction (for performance testing)")
	checkChangeSetsCmd.Flags().BoolVar(&transactionsV3, "experimental.transactions.v3", false, "(this flag is in testing stage) Not recommended yet: Can't change this flag after node creation. New DB table for transactions allows keeping multiple branches of block bodies in the DB simultaneously")
	rootCmd.AddCommand(checkChangeSetsCmd)
}

var checkChangeSetsCmd = &cobra.Command{
	Use:   "checkChangeSets",
	Short: "Re-executes historical transactions in read-only mode and checks that their outputs match the database ChangeSets",
	RunE: func(cmd *cobra.Command, args []string) error {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "check_change_sets"); err != nil {
			logger.Error("Setting up", "error", err)
			return err
		}
		return CheckChangeSets(genesis, logger, block, chaindata, historyfile, nocheck, transactionsV3)
	},
}

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func CheckChangeSets(genesis *types.Genesis, logger log.Logger, blockNum uint64, chaindata string, historyfile string, nocheck bool, transactionV3 bool) error {
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
	allSnapshots := snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadirCli, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(allSnapshots, transactionV3)

	chainDb := db
	defer chainDb.Close()
	historyDb := chainDb
	if chaindata != historyfile {
		historyDb = kv2.MustOpen(historyfile)
	}
	ctx := context.Background()
	historyTx, err1 := historyDb.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	noOpWriter := state.NewNoopWriter()

	interrupt := false
	rwtx, err := chainDb.BeginRw(ctx)
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

	engine := initConsensusEngine(chainConfig, allSnapshots)

	for !interrupt {

		if blockNum > execAt {
			log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than Exec stage=%d", blockNum, execAt))
			break
		}
		if blockNum > historyAt {
			log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than History stage=%d", blockNum, historyAt))
			break
		}

		blockHash, err := blockReader.CanonicalHash(ctx, historyTx, blockNum)
		if err != nil {
			return err
		}
		var b *types.Block
		b, _, err = blockReader.BlockWithSenders(ctx, historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		reader := state.NewPlainState(historyTx, blockNum, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
		//reader.SetTrace(blockNum == uint64(block))
		intraBlockState := state.New(reader)
		csw := state.NewChangeSetWriterPlain(nil /* db */, blockNum)
		var blockWriter state.StateWriter
		if nocheck {
			blockWriter = noOpWriter
		} else {
			blockWriter = csw
		}

		getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
			h, e := blockReader.Header(ctx, rwtx, hash, number)
			if e != nil {
				panic(e)
			}
			return h
		}
		receipts, err1 := runBlock(engine, intraBlockState, noOpWriter, blockWriter, chainConfig, getHeader, b, vmConfig, blockNum == block)
		if err1 != nil {
			return err1
		}
		if chainConfig.IsByzantium(blockNum) {
			receiptSha := types.DeriveSha(receipts)
			if receiptSha != b.ReceiptHash() {
				return fmt.Errorf("mismatched receipt headers for block %d", blockNum)
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
			err = historyv2.ForPrefix(historyTx, kv.AccountChangeSet, hexutility.EncodeTs(blockNum), func(blockN uint64, k, v []byte) error {
				if i >= len(accountChanges.Changes) {
					if len(v) != 0 {
						fmt.Printf("Unexpected account changes in block %d\n", blockNum)
						fmt.Printf("In the database: ======================\n")
						fmt.Printf("%d: 0x%x: %x\n", i, k, v)
						match = false
					}
					i++
					return nil
				}
				c := accountChanges.Changes[i]
				if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
					i++
					return nil
				}
				if len(v) == 0 {
					return nil
				}

				match = false
				fmt.Printf("Unexpected account changes in block %d\n", blockNum)
				fmt.Printf("In the database: ======================\n")
				fmt.Printf("%d: 0x%x: %x\n", i, k, v)
				fmt.Printf("Expected: ==========================\n")
				fmt.Printf("%d: 0x%x %x\n", i, c.Key, c.Value)
				i++
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
				expectedStorageChanges = historyv2.NewChangeSet()
			}
			sort.Sort(expectedStorageChanges)
			match = true
			err = historyv2.ForPrefix(historyTx, kv.StorageChangeSet, hexutility.EncodeTs(blockNum), func(blockN uint64, k, v []byte) error {
				if i >= len(expectedStorageChanges.Changes) {
					fmt.Printf("Unexpected storage changes in block %d\nIn the database: ======================\n", blockNum)
					fmt.Printf("0x%x: %x\n", k, v)
					match = false
					i++
					return nil
				}
				c := expectedStorageChanges.Changes[i]
				i++
				if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
					return nil
				}
				match = false
				fmt.Printf("Unexpected storage changes in block %d\nIn the database: ======================\n", blockNum)
				fmt.Printf("0x%x: %x\n", k, v)
				fmt.Printf("Expected: ==========================\n")
				fmt.Printf("0x%x %x\n", c.Key, c.Value)
				i++
				return nil
			})
			if err != nil {
				return err
			}
			if !match {
				return fmt.Errorf("check change set failed")
			}
		}

		blockNum++
		if blockNum%1000 == 0 {
			logger.Info("Checked", "blocks", blockNum)
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	logger.Info("Checked", "blocks", blockNum, "next time specify --block", blockNum, "duration", time.Since(startTime))
	return nil
}
