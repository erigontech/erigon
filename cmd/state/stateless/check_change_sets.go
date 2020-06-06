package stateless

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func CheckChangeSets(genesis *core.Genesis, blockNum uint64, chaindata string, historyfile string, nocheck bool) error {
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

	chainDb, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		return err
	}
	defer chainDb.Close()
	historyDb := chainDb
	if chaindata != historyfile {
		historyDb, err = ethdb.NewBoltDatabase(historyfile)
		if err != nil {
			return err
		}
	}

	chainConfig := genesis.Config
	engine := ethash.NewFaker()
	vmConfig := vm.Config{}
	dests := vm.NewDestsCache(100)
	bc, err := core.NewBlockChain(chainDb, nil, chainConfig, engine, vmConfig, nil, nil, dests)
	if err != nil {
		return err
	}

	noOpWriter := state.NewNoopWriter()

	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}

		dbstate := state.NewDbState(historyDb.KV(), block.NumberU64()-1)
		intraBlockState := state.New(dbstate)
		csw := state.NewChangeSetWriter()
		var blockWriter state.StateWriter
		if nocheck {
			blockWriter = noOpWriter
		} else {
			blockWriter = csw
		}

		if err := runBlock(intraBlockState, noOpWriter, blockWriter, chainConfig, bc, block); err != nil {
			return err
		}

		if !nocheck {
			accountChanges, err := csw.GetAccountChanges()
			if err != nil {
				return err
			}
			var expectedAccountChanges []byte
			expectedAccountChanges, err = changeset.EncodeAccounts(accountChanges)
			if err != nil {
				return err
			}

			dbAccountChanges, err := historyDb.GetChangeSetByBlock(dbutils.AccountsHistoryBucket, blockNum)
			if err != nil {
				return err
			}

			if !bytes.Equal(dbAccountChanges, expectedAccountChanges) {
				fmt.Printf("Unexpected account changes in block %d\n%s\nvs\n%s\n", blockNum, hexutil.Encode(dbAccountChanges), hexutil.Encode(expectedAccountChanges))
				csw.PrintChangedAccounts()
				return nil
			}

			expectedStorageChanges, err := csw.GetStorageChanges()
			if err != nil {
				return err
			}
			expectedtorageSerialized := make([]byte, 0)
			if expectedStorageChanges.Len() > 0 {
				expectedtorageSerialized, err = changeset.EncodeStorage(expectedStorageChanges)
				if err != nil {
					return err
				}
			}

			dbStorageChanges, err := historyDb.GetChangeSetByBlock(dbutils.StorageHistoryBucket, blockNum)
			if err != nil {
				return err
			}

			if !bytes.Equal(dbStorageChanges, expectedtorageSerialized) {
				fmt.Printf("Unexpected storage changes in block %d\n%s\nvs\n%s\n", blockNum, hexutil.Encode(dbStorageChanges), hexutil.Encode(expectedtorageSerialized))
				return nil
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

	log.Info("Checked", "blocks", blockNum, "next time specify --block", blockNum, "duration", time.Since(startTime))
	return nil
}
