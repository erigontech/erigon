package stateless

import (
	"bufio"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

//nolint:deadcode,unused
func feemarket(blockNum uint64) {
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	ethDb, err := ethdb.NewBoltDatabase("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	//ethDb, err := ethdb.NewBoltDatabase("/home/akhounov/.ethereum/geth/chaindata1")
	check(err)
	defer ethDb.Close()
	chainConfig := params.MainnetChainConfig
	fmFile, err := os.OpenFile("fee_market.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer fmFile.Close()
	w := bufio.NewWriter(fmFile)
	defer w.Flush()
	vmConfig := vm.Config{}
	engine := ethash.NewFullFaker()
	bcb, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vmConfig, nil, nil, nil)
	check(err)
	interrupt := false
	txCount := 0
	MinFeeMaxChangeDenominator := big.NewInt(8)
	parentMinFee := big.NewInt(1000000000)
	minimum := big.NewInt(1000)
	for !interrupt {
		block := bcb.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		header := block.Header()
		targetGasUsed := big.NewInt(int64(header.GasLimit))
		targetGasUsed.Div(targetGasUsed, big.NewInt(2))
		delta := big.NewInt(int64(header.GasUsed))
		delta.Sub(delta, targetGasUsed)
		minFee := big.NewInt(0).Mul(parentMinFee, delta)
		minFee.Div(minFee, targetGasUsed)
		minFee.Div(minFee, MinFeeMaxChangeDenominator)
		minFee.Add(minFee, parentMinFee)
		if minFee.Cmp(minimum) < 0 {
			minFee.Set(minimum)
		}
		for range block.Transactions() {
			txCount++
		}
		fmt.Fprintf(w, "%d,%d\n", blockNum, minFee)
		blockNum++
		parentMinFee = minFee
		if blockNum%1000 == 0 {
			//tds.PruneTries(true)
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Fee market analysis took %s\n", time.Since(startTime))
}
