package stateless

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func CheckChangeSets(blockNum uint64, chaindata string) error {
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		return err
	}
	defer ethDb.Close()

	chainConfig := params.MainnetChainConfig
	engine := ethash.NewFaker()
	vmConfig := vm.Config{}
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vmConfig, nil)
	if err != nil {
		return err
	}

	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}

		dbstate := state.NewDbState(ethDb, block.NumberU64()-1)
		intraBlockState := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())

		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, intraBlockState, chainConfig, vmConfig)
			if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}

			// TODO [Andrew] generate change set from intraBlockState and compare against DB
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
