package stateless

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func CheckChangeSets(genesis *core.Genesis, blockNum uint64, chaindata string, historyfile string, nocheck bool, writeReceipts bool) error {
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

	chainDb := ethdb.MustOpen(chaindata)
	defer chainDb.Close()
	historyDb := chainDb
	if chaindata != historyfile {
		historyDb = ethdb.MustOpen(historyfile)
	}
	historyTx, err1 := historyDb.KV().Begin(context.Background(), nil, ethdb.RO)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	chainConfig := genesis.Config
	engine := ethash.NewFaker()
	vmConfig := vm.Config{}
	cc := &core.TinyChainContext{}
	cc.SetDB(chainDb)
	cc.SetEngine(engine)

	noOpWriter := state.NewNoopWriter()

	interrupt := false
	batch := chainDb.NewBatch()
	defer batch.Rollback()

	execAt, _, err1 := stages.GetStageProgress(chainDb, stages.Execution)
	if err1 != nil {
		return err1
	}
	historyAt, _, err1 := stages.GetStageProgress(chainDb, stages.StorageHistoryIndex)
	if err1 != nil {
		return err1
	}

	for !interrupt {
		if blockNum > execAt {
			log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than Exec stage=%d", blockNum, execAt))
			break
		}
		if blockNum > historyAt {
			log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than History stage=%d", blockNum, historyAt))
			break
		}

		blockHash, err := rawdb.ReadCanonicalHash(chainDb, blockNum)
		if err != nil {
			return err
		}
		block := rawdb.ReadBlock(chainDb, blockHash, blockNum)
		if block == nil {
			break
		}

		dbstate := state.NewPlainDBState(historyTx, block.NumberU64()-1)
		intraBlockState := state.New(dbstate)
		csw := state.NewChangeSetWriterPlain(nil /* db */, block.NumberU64()-1)
		var blockWriter state.StateWriter
		if nocheck {
			blockWriter = noOpWriter
		} else {
			blockWriter = csw
		}

		receipts, err1 := runBlock(intraBlockState, noOpWriter, blockWriter, chainConfig, cc, block, vmConfig)
		if err1 != nil {
			return err1
		}
		if writeReceipts {
			if chainConfig.IsByzantium(block.Number()) {
				receiptSha := types.DeriveSha(receipts)
				if receiptSha != block.Header().ReceiptHash {
					return fmt.Errorf("mismatched receipt headers for block %d", block.NumberU64())
				}
			}

			if err := rawdb.AppendReceipts(batch, block.NumberU64(), receipts); err != nil {
				return err
			}
			if batch.BatchSize() >= batch.IdealBatchSize() {
				log.Info("Committing receipts", "up to block", block.NumberU64(), "batch size", common.StorageSize(batch.BatchSize()))
				if err := batch.CommitAndBegin(context.Background()); err != nil {
					return err
				}
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
			err = changeset.Walk(historyDb, dbutils.PlainAccountChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
				c := accountChanges.Changes[i]
				if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
					i++
					return true, nil
				}

				match = false
				fmt.Printf("Unexpected account changes in block %d\n", blockNum)
				fmt.Printf("In the database: ======================\n")
				fmt.Printf("%d: 0x%x: %x\n", i, k, v)
				fmt.Printf("Expected: ==========================\n")
				fmt.Printf("%d: 0x%x %x\n", i, c.Key, c.Value)
				return false, nil
			})
			if err != nil {
				return err
			}

			if !match {
				//fmt.Printf("\n\n")
				//fmt.Printf("All in DB: ==========================\n")
				//j := 0
				//err = changeset.Walk(historyDb, dbutils.PlainAccountChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
				//	fmt.Printf("%d: 0x%x: %x\n", j, k, v)
				//	j++
				//	return true, nil
				//})
				//if err != nil {
				//	return err
				//}
				//fmt.Printf("All Expected: ==========================\n")
				//for ii, c := range accountChanges.Changes {
				//	fmt.Printf("%d: 0x%x: %x\n", ii, c.Key, c.Value)
				//}

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
			err = changeset.Walk(historyDb, dbutils.PlainStorageChangeSetBucket, dbutils.EncodeBlockNumber(blockNum), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
				c := expectedStorageChanges.Changes[i]
				i++
				if bytes.Equal(c.Key, k) && bytes.Equal(c.Value, v) {
					return false, nil
				}

				fmt.Printf("Unexpected storage changes in block %d\nIn the database: ======================\n", blockNum)
				fmt.Printf("0x%x: %x\n", k, v)
				fmt.Printf("Expected: ==========================\n")
				fmt.Printf("0x%x %x\n", c.Key, c.Value)
				return true, fmt.Errorf("check change set failed")
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
		default:
		}
	}
	if writeReceipts {
		log.Info("Committing final receipts", "batch size", common.StorageSize(batch.BatchSize()))
		if _, err := batch.Commit(); err != nil {
			return err
		}
	}
	log.Info("Checked", "blocks", blockNum, "next time specify --block", blockNum, "duration", time.Since(startTime))
	return nil
}
