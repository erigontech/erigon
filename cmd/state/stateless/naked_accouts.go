package stateless

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/wcharczuk/go-chart"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

type AccountsTracer struct {
	loaded      map[common.Address]struct{}
	totalWrites int
	nakedWrites int
	totalReads  int
	nakedReads  int
}

func NewAccountsTracer() *AccountsTracer {
	return &AccountsTracer{
		loaded: make(map[common.Address]struct{}),
	}
}

func (at *AccountsTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (at *AccountsTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (at *AccountsTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (at *AccountsTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (at *AccountsTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (at *AccountsTracer) CaptureAccountRead(account common.Address) error {
	if _, ok := at.loaded[account]; !ok {
		at.nakedReads++
		at.loaded[account] = struct{}{}
	}
	at.totalReads++
	return nil
}
func (at *AccountsTracer) CaptureAccountWrite(account common.Address) error {
	if _, ok := at.loaded[account]; !ok {
		at.nakedWrites++
		at.loaded[account] = struct{}{}
	}
	at.totalWrites++
	return nil
}

//nolint:deadcode,unused
func accountsReadWrites(blockNum uint64) {
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ethDb := ethdb.MustOpen("/Volumes/tb41/turbo-geth-10/geth/chaindata")
	defer ethDb.Close()
	ethTx, err1 := ethDb.KV().Begin(context.Background(), nil, false)
	check(err1)
	defer ethTx.Rollback()
	chainConfig := params.MainnetChainConfig
	srwFile, err := os.OpenFile("/Volumes/tb41/turbo-geth/account_read_writes.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer srwFile.Close()
	w := bufio.NewWriter(srwFile)
	defer w.Flush()
	at := NewAccountsTracer()
	vmConfig := vm.Config{Tracer: at, Debug: false}
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil, txCacher)
	check(err)
	defer bc.Stop()
	interrupt := false
	totalWrites := 0
	nakedWrites := 0
	totalReads := 0
	nakedReads := 0
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewPlainDBState(ethTx, block.NumberU64()-1)
		statedb := state.New(dbstate)
		statedb.SetTracer(at)
		signer := types.MakeSigner(chainConfig, block.Number())
		at.loaded = make(map[common.Address]struct{})
		at.totalWrites = 0
		at.nakedWrites = 0
		at.totalReads = 0
		at.nakedReads = 0
		for _, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			}
		}
		fmt.Fprintf(w, "%d,%d,%d,%d,%d\n", blockNum, at.totalWrites, at.nakedWrites, at.totalReads, at.nakedReads)
		totalWrites += at.totalWrites
		nakedWrites += at.nakedWrites
		totalReads += at.totalReads
		nakedReads += at.nakedReads
		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Processed %d blocks, totalWrites %d, nakedWrites %d, totalReads %d, nakedReads %d\n", blockNum, totalWrites, nakedWrites, totalReads, nakedReads)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Processed %d blocks, totalWrites %d, nakedWrites %d, totalReads %d, nakedReads %d\n", blockNum, totalWrites, nakedWrites, totalReads, nakedReads)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Accounts read/write analysis took %s\n", time.Since(startTime))
}

func nakedAccountChart() {
	accFile, err := os.Open("/Volumes/tb4/turbo-geth/account_read_writes.csv")
	check(err)
	defer accFile.Close()
	accReader := csv.NewReader(bufio.NewReader(accFile))
	var blocks []float64
	var totalReads []float64
	var nakedReads []float64
	for records, _ := accReader.Read(); records != nil; records, _ = accReader.Read() {
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		totalReads = append(totalReads, parseFloat64(records[3]))
		nakedReads = append(nakedReads, parseFloat64(records[4]))
	}
	var totalReadsGroup float64 = 0
	var nakedReadsGroup float64 = 0
	var i int
	var window int = 1024
	b := make([]float64, len(blocks)-window+1)
	ts := make([]float64, len(blocks)-window+1)
	ns := make([]float64, len(blocks)-window+1)
	for i = 0; i < len(blocks); i++ {
		totalReadsGroup += totalReads[i]
		nakedReadsGroup += nakedReads[i]
		if i >= window {
			totalReadsGroup -= totalReads[i-window]
			nakedReadsGroup -= nakedReads[i-window]
		}
		if i >= window-1 {
			b[i-window+1] = blocks[i]
			ts[i-window+1] = totalReadsGroup / float64(window)
			ns[i-window+1] = nakedReadsGroup / float64(window)
		}
	}
	totalSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Total account reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorYellow,
			FillColor:   chart.ColorYellow.WithAlpha(100),
		},
		XValues: b,
		YValues: ts,
	}
	nakedSeries := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked account reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		XValues: b,
		YValues: ns,
	}
	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "operations",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: operations(),
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			totalSeries,
			nakedSeries,
		},
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile("naked_reads.png", buffer.Bytes(), 0644)
	check(err)
	totalSeries2 := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Total account reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorYellow,
			FillColor:   chart.ColorYellow.WithAlpha(100),
		},
		XValues: b[4000000:],
		YValues: ts[4000000:],
	}
	nakedSeries2 := &chart.ContinuousSeries{
		Name: fmt.Sprintf("Naked account reads per block, moving average with window %d", window),
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.ColorBlack,
		},
		XValues: b[4000000:],
		YValues: ns[4000000:],
	}
	graph2 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "operations",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.1f", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: operations(),
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			GridLines: blockMillions(),
		},
		Series: []chart.Series{
			totalSeries2,
			nakedSeries2,
		},
	}

	graph2.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer2 := bytes.NewBuffer([]byte{})
	err = graph2.Render(chart.PNG, buffer2)
	check(err)
	err = ioutil.WriteFile("naked_reads_4m.png", buffer2.Bytes(), 0644)
	check(err)
}
