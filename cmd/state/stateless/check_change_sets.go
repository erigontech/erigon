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
	"github.com/ledgerwatch/turbo-geth/params"
)

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func CheckChangeSets(blockNum uint64, chaindata string, statefile string) error {
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
	stateDb := chainDb
	if chaindata != statefile {
		stateDb, err = ethdb.NewBoltDatabase(statefile)
		if err != nil {
			return err
		}
	}

	chainConfig := params.MainnetChainConfig
	engine := ethash.NewFaker()
	vmConfig := vm.Config{}
	bc, err := core.NewBlockChain(chainDb, nil, chainConfig, engine, vmConfig, nil)
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

		dbstate := state.NewDbState(stateDb, block.NumberU64()-1)
		intraBlockState := state.New(dbstate)
		csw := state.NewChangeSetWriter()

		if err := runBlock(intraBlockState, noOpWriter, csw, chainConfig, bc, block); err != nil {
			return err
		}

		expectedAccountChanges, err := changeset.EncodeChangeSet(csw.GetAccountChanges())
		if err != nil {
			return err
		}

		dbAccountChanges, err := stateDb.GetChangeSetByBlock(dbutils.AccountsHistoryBucket, blockNum)
		if err != nil {
			return err
		}

		if !bytes.Equal(dbAccountChanges, expectedAccountChanges) {
			fmt.Printf("Unexpected account changes in block %d\n%s\nvs\n%s\n", blockNum, hexutil.Encode(dbAccountChanges), hexutil.Encode(expectedAccountChanges))
			csw.PrintChangedAccounts()
			return nil
		}

		expectedStorageChanges := csw.GetStorageChanges()
		expectedtorageSerialized := make([]byte, 0)
		if expectedStorageChanges.Len() > 0 {
			expectedtorageSerialized, err = changeset.EncodeChangeSet(expectedStorageChanges)
			if err != nil {
				return err
			}
		}

		dbStorageChanges, err := stateDb.GetChangeSetByBlock(dbutils.StorageHistoryBucket, blockNum)
		if err != nil {
			return err
		}

		if !bytes.Equal(dbStorageChanges, expectedtorageSerialized) {
			fmt.Printf("Unexpected storage changes in block %d\n%s\nvs\n%s\n", blockNum, hexutil.Encode(dbStorageChanges), hexutil.Encode(expectedtorageSerialized))
			return nil
		}

		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Checked %d blocks\n", blockNum)
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}

	fmt.Printf("Checked %d blocks\n", blockNum)
	fmt.Printf("Next time specify --block %d\n", blockNum)
	fmt.Printf("CheckChangeSets took %s\n", time.Since(startTime))

	return nil
}
